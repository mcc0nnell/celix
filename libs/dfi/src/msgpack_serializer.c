/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "msgpack_serializer.h"
#include "celix_array_list_encoding.h"
#include "celix_err.h"
#include "celix_properties.h"
#include "dyn_type_common.h"
#include "json_serializer.h"

#include <jansson.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CELIX_MSGPACK_MAX_DEPTH 64u
typedef struct celix_msgpack_buffer {
    uint8_t* data;
    size_t size;
    size_t capacity;
} celix_msgpack_buffer_t;

typedef struct celix_msgpack_cursor {
    const uint8_t* data;
    size_t size;
    size_t pos;
} celix_msgpack_cursor_t;

static int msgpack_reserve(celix_msgpack_buffer_t* buf, size_t extra) {
    if (extra > SIZE_MAX - buf->size) {
        celix_err_push("MessagePack buffer size overflow");
        return 1;
    }
    size_t needed = buf->size + extra;
    if (needed <= buf->capacity) {
        return 0;
    }
    size_t cap = buf->capacity == 0 ? 128 : buf->capacity;
    while (cap < needed) {
        if (cap > SIZE_MAX / 2) {
            cap = needed;
            break;
        }
        cap *= 2;
    }
    uint8_t* data = realloc(buf->data, cap);
    if (data == NULL) {
        celix_err_push("Cannot allocate MessagePack output buffer");
        return 1;
    }
    buf->data = data;
    buf->capacity = cap;
    return 0;
}

static int msgpack_append(celix_msgpack_buffer_t* buf, const void* data, size_t len) {
    if (msgpack_reserve(buf, len) != 0) {
        return 1;
    }
    memcpy(buf->data + buf->size, data, len);
    buf->size += len;
    return 0;
}

static int msgpack_put_u8(celix_msgpack_buffer_t* buf, uint8_t v) { return msgpack_append(buf, &v, 1); }

static int msgpack_put_u16(celix_msgpack_buffer_t* buf, uint16_t v) {
    uint8_t bytes[2] = {(uint8_t)(v >> 8), (uint8_t)v};
    return msgpack_append(buf, bytes, sizeof(bytes));
}

static int msgpack_put_u32(celix_msgpack_buffer_t* buf, uint32_t v) {
    uint8_t bytes[4] = {(uint8_t)(v >> 24), (uint8_t)(v >> 16), (uint8_t)(v >> 8), (uint8_t)v};
    return msgpack_append(buf, bytes, sizeof(bytes));
}

static int msgpack_put_u64(celix_msgpack_buffer_t* buf, uint64_t v) {
    uint8_t bytes[8] = {(uint8_t)(v >> 56),
                        (uint8_t)(v >> 48),
                        (uint8_t)(v >> 40),
                        (uint8_t)(v >> 32),
                        (uint8_t)(v >> 24),
                        (uint8_t)(v >> 16),
                        (uint8_t)(v >> 8),
                        (uint8_t)v};
    return msgpack_append(buf, bytes, sizeof(bytes));
}

static int msgpack_pack_json(celix_msgpack_buffer_t* buf, json_t* value, unsigned depth);

static int msgpack_pack_string(celix_msgpack_buffer_t* buf, const char* str, size_t len) {
    int status = 0;
    if (len <= 31) {
        status = msgpack_put_u8(buf, (uint8_t)(0xa0u | (uint8_t)len));
    } else if (len <= UINT8_MAX) {
        status = msgpack_put_u8(buf, 0xd9);
        if (status == 0)
            status = msgpack_put_u8(buf, (uint8_t)len);
    } else if (len <= UINT16_MAX) {
        status = msgpack_put_u8(buf, 0xda);
        if (status == 0)
            status = msgpack_put_u16(buf, (uint16_t)len);
    } else if (len <= UINT32_MAX) {
        status = msgpack_put_u8(buf, 0xdb);
        if (status == 0)
            status = msgpack_put_u32(buf, (uint32_t)len);
    } else {
        celix_err_push("MessagePack string too large");
        return 1;
    }
    return status == 0 ? msgpack_append(buf, str, len) : status;
}

static int msgpack_pack_integer(celix_msgpack_buffer_t* buf, json_int_t val) {
    if (val >= 0) {
        uint64_t v = (uint64_t)val;
        if (v <= 0x7f)
            return msgpack_put_u8(buf, (uint8_t)v);
        if (v <= UINT8_MAX)
            return msgpack_put_u8(buf, 0xcc) || msgpack_put_u8(buf, (uint8_t)v);
        if (v <= UINT16_MAX)
            return msgpack_put_u8(buf, 0xcd) || msgpack_put_u16(buf, (uint16_t)v);
        if (v <= UINT32_MAX)
            return msgpack_put_u8(buf, 0xce) || msgpack_put_u32(buf, (uint32_t)v);
        return msgpack_put_u8(buf, 0xcf) || msgpack_put_u64(buf, v);
    }
    if (val >= -32)
        return msgpack_put_u8(buf, (uint8_t)(int8_t)val);
    if (val >= INT8_MIN)
        return msgpack_put_u8(buf, 0xd0) || msgpack_put_u8(buf, (uint8_t)(int8_t)val);
    if (val >= INT16_MIN)
        return msgpack_put_u8(buf, 0xd1) || msgpack_put_u16(buf, (uint16_t)(int16_t)val);
    if (val >= INT32_MIN)
        return msgpack_put_u8(buf, 0xd2) || msgpack_put_u32(buf, (uint32_t)(int32_t)val);
    return msgpack_put_u8(buf, 0xd3) || msgpack_put_u64(buf, (uint64_t)(int64_t)val);
}
static int msgpack_pack_array(celix_msgpack_buffer_t* buf, json_t* value, unsigned depth) {
    size_t len = json_array_size(value);
    int status = 0;
    if (len <= 15) {
        status = msgpack_put_u8(buf, (uint8_t)(0x90u | (uint8_t)len));
    } else if (len <= UINT16_MAX) {
        status = msgpack_put_u8(buf, 0xdc);
        if (status == 0)
            status = msgpack_put_u16(buf, (uint16_t)len);
    } else if (len <= UINT32_MAX) {
        status = msgpack_put_u8(buf, 0xdd);
        if (status == 0)
            status = msgpack_put_u32(buf, (uint32_t)len);
    } else {
        celix_err_push("MessagePack array too large");
        return 1;
    }
    for (size_t i = 0; status == 0 && i < len; ++i) {
        status = msgpack_pack_json(buf, json_array_get(value, i), depth + 1);
    }
    return status;
}

static int msgpack_pack_map(celix_msgpack_buffer_t* buf, json_t* value, unsigned depth) {
    size_t len = json_object_size(value);
    int status = 0;
    if (len <= 15) {
        status = msgpack_put_u8(buf, (uint8_t)(0x80u | (uint8_t)len));
    } else if (len <= UINT16_MAX) {
        status = msgpack_put_u8(buf, 0xde);
        if (status == 0)
            status = msgpack_put_u16(buf, (uint16_t)len);
    } else if (len <= UINT32_MAX) {
        status = msgpack_put_u8(buf, 0xdf);
        if (status == 0)
            status = msgpack_put_u32(buf, (uint32_t)len);
    } else {
        celix_err_push("MessagePack map too large");
        return 1;
    }
    const char* key = NULL;
    json_t* entry = NULL;
    json_object_foreach(value, key, entry) {
        if (status != 0)
            break;
        status = msgpack_pack_string(buf, key, strlen(key));
        if (status == 0)
            status = msgpack_pack_json(buf, entry, depth + 1);
    }
    return status;
}

static int msgpack_pack_json(celix_msgpack_buffer_t* buf, json_t* value, unsigned depth) {
    if (depth > CELIX_MSGPACK_MAX_DEPTH) {
        celix_err_push("MessagePack nesting too deep");
        return 1;
    }
    if (json_is_null(value))
        return msgpack_put_u8(buf, 0xc0);
    if (json_is_true(value))
        return msgpack_put_u8(buf, 0xc3);
    if (json_is_false(value))
        return msgpack_put_u8(buf, 0xc2);
    if (json_is_integer(value))
        return msgpack_pack_integer(buf, json_integer_value(value));
    if (json_is_string(value))
        return msgpack_pack_string(buf, json_string_value(value), json_string_length(value));
    if (json_is_real(value)) {
        double d = json_real_value(value);
        uint64_t bits = 0;
        memcpy(&bits, &d, sizeof(bits));
        return msgpack_put_u8(buf, 0xcb) || msgpack_put_u64(buf, bits);
    }
    if (json_is_array(value))
        return msgpack_pack_array(buf, value, depth);
    if (json_is_object(value))
        return msgpack_pack_map(buf, value, depth);
    celix_err_push("Unsupported JSON value for MessagePack serialization");
    return 1;
}

static const uint8_t* msgpack_take(celix_msgpack_cursor_t* cur, size_t len) {
    if (len > cur->size - cur->pos) {
        celix_err_push("Truncated MessagePack input");
        return NULL;
    }
    const uint8_t* result = cur->data + cur->pos;
    cur->pos += len;
    return result;
}

static int msgpack_get_u8(celix_msgpack_cursor_t* cur, uint8_t* out) {
    const uint8_t* p = msgpack_take(cur, 1);
    if (p == NULL)
        return 1;
    *out = p[0];
    return 0;
}

static int msgpack_get_u16(celix_msgpack_cursor_t* cur, uint16_t* out) {
    const uint8_t* p = msgpack_take(cur, 2);
    if (p == NULL)
        return 1;
    *out = ((uint16_t)p[0] << 8) | p[1];
    return 0;
}

static int msgpack_get_u32(celix_msgpack_cursor_t* cur, uint32_t* out) {
    const uint8_t* p = msgpack_take(cur, 4);
    if (p == NULL)
        return 1;
    *out = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
    return 0;
}

static int msgpack_get_u64(celix_msgpack_cursor_t* cur, uint64_t* out) {
    const uint8_t* p = msgpack_take(cur, 8);
    if (p == NULL)
        return 1;
    *out = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) | ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
           ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) | ((uint64_t)p[6] << 8) | p[7];
    return 0;
}

static json_t* msgpack_unpack_json(celix_msgpack_cursor_t* cur, unsigned depth);

static json_t* msgpack_unpack_string(celix_msgpack_cursor_t* cur, uint32_t len) {
    const uint8_t* p = msgpack_take(cur, len);
    if (p == NULL)
        return NULL;
    json_t* value = json_stringn((const char*)p, len);
    if (value == NULL) {
        celix_err_push("Invalid UTF-8 string in MessagePack input");
    }
    return value;
}

static json_t* msgpack_unpack_array(celix_msgpack_cursor_t* cur, uint32_t len, unsigned depth) {
    json_t* array = json_array();
    if (array == NULL)
        return NULL;
    for (uint32_t i = 0; i < len; ++i) {
        json_t* value = msgpack_unpack_json(cur, depth + 1);
        if (value == NULL || json_array_append_new(array, value) != 0) {
            json_decref(array);
            return NULL;
        }
    }
    return array;
}

static json_t* msgpack_unpack_map(celix_msgpack_cursor_t* cur, uint32_t len, unsigned depth) {
    json_t* object = json_object();
    if (object == NULL)
        return NULL;
    for (uint32_t i = 0; i < len; ++i) {
        json_t* key = msgpack_unpack_json(cur, depth + 1);
        if (key == NULL || !json_is_string(key)) {
            json_decref(key);
            json_decref(object);
            celix_err_push("MessagePack map key must be a string");
            return NULL;
        }
        json_t* value = msgpack_unpack_json(cur, depth + 1);
        if (value == NULL || json_object_set_new(object, json_string_value(key), value) != 0) {
            json_decref(key);
            json_decref(object);
            return NULL;
        }
        json_decref(key);
    }
    return object;
}

static json_t* msgpack_json_integer_signed(int64_t value) { return json_integer((json_int_t)value); }

static json_t* msgpack_json_integer_unsigned(uint64_t value) {
    if (value > (uint64_t)INT64_MAX) {
        celix_err_push("MessagePack unsigned integer exceeds Celix JSON integer range");
        return NULL;
    }
    return json_integer((json_int_t)value);
}

static json_t* msgpack_unpack_json(celix_msgpack_cursor_t* cur, unsigned depth) {
    if (depth > CELIX_MSGPACK_MAX_DEPTH) {
        celix_err_push("MessagePack nesting too deep");
        return NULL;
    }
    uint8_t tag = 0;
    if (msgpack_get_u8(cur, &tag) != 0)
        return NULL;
    if (tag <= 0x7f)
        return msgpack_json_integer_unsigned(tag);
    if (tag >= 0xe0)
        return msgpack_json_integer_signed((int8_t)tag);
    if ((tag & 0xe0u) == 0xa0u)
        return msgpack_unpack_string(cur, tag & 0x1fu);
    if ((tag & 0xf0u) == 0x90u)
        return msgpack_unpack_array(cur, tag & 0x0fu, depth);
    if ((tag & 0xf0u) == 0x80u)
        return msgpack_unpack_map(cur, tag & 0x0fu, depth);

    switch (tag) {
    case 0xc0:
        return json_null();
    case 0xc2:
        return json_false();
    case 0xc3:
        return json_true();
    case 0xcc: {
        uint8_t v;
        return msgpack_get_u8(cur, &v) == 0 ? msgpack_json_integer_unsigned(v) : NULL;
    }
    case 0xcd: {
        uint16_t v;
        return msgpack_get_u16(cur, &v) == 0 ? msgpack_json_integer_unsigned(v) : NULL;
    }
    case 0xce: {
        uint32_t v;
        return msgpack_get_u32(cur, &v) == 0 ? msgpack_json_integer_unsigned(v) : NULL;
    }
    case 0xcf: {
        uint64_t v;
        return msgpack_get_u64(cur, &v) == 0 ? msgpack_json_integer_unsigned(v) : NULL;
    }
    case 0xd0: {
        uint8_t v;
        return msgpack_get_u8(cur, &v) == 0 ? msgpack_json_integer_signed((int8_t)v) : NULL;
    }
    case 0xd1: {
        uint16_t v;
        return msgpack_get_u16(cur, &v) == 0 ? msgpack_json_integer_signed((int16_t)v) : NULL;
    }
    case 0xd2: {
        uint32_t v;
        return msgpack_get_u32(cur, &v) == 0 ? msgpack_json_integer_signed((int32_t)v) : NULL;
    }
    case 0xd3: {
        uint64_t v;
        return msgpack_get_u64(cur, &v) == 0 ? msgpack_json_integer_signed((int64_t)v) : NULL;
    }
    case 0xca: {
        uint32_t bits;
        if (msgpack_get_u32(cur, &bits) != 0)
            return NULL;
        float f;
        memcpy(&f, &bits, sizeof(f));
        return json_real((double)f);
    }
    case 0xcb: {
        uint64_t bits;
        if (msgpack_get_u64(cur, &bits) != 0)
            return NULL;
        double d;
        memcpy(&d, &bits, sizeof(d));
        return json_real(d);
    }
    case 0xd9: {
        uint8_t len;
        return msgpack_get_u8(cur, &len) == 0 ? msgpack_unpack_string(cur, len) : NULL;
    }
    case 0xda: {
        uint16_t len;
        return msgpack_get_u16(cur, &len) == 0 ? msgpack_unpack_string(cur, len) : NULL;
    }
    case 0xdb: {
        uint32_t len;
        return msgpack_get_u32(cur, &len) == 0 ? msgpack_unpack_string(cur, len) : NULL;
    }
    case 0xdc: {
        uint16_t len;
        return msgpack_get_u16(cur, &len) == 0 ? msgpack_unpack_array(cur, len, depth) : NULL;
    }
    case 0xdd: {
        uint32_t len;
        return msgpack_get_u32(cur, &len) == 0 ? msgpack_unpack_array(cur, len, depth) : NULL;
    }
    case 0xde: {
        uint16_t len;
        return msgpack_get_u16(cur, &len) == 0 ? msgpack_unpack_map(cur, len, depth) : NULL;
    }
    case 0xdf: {
        uint32_t len;
        return msgpack_get_u32(cur, &len) == 0 ? msgpack_unpack_map(cur, len, depth) : NULL;
    }
    default:
        celix_err_pushf("Unsupported MessagePack tag 0x%02x", tag);
        return NULL;
    }
}

static int msgpack_pack_array_header(celix_msgpack_buffer_t* buf, size_t len) {
    if (len <= 15)
        return msgpack_put_u8(buf, (uint8_t)(0x90u | (uint8_t)len));
    if (len <= UINT16_MAX)
        return msgpack_put_u8(buf, 0xdc) || msgpack_put_u16(buf, (uint16_t)len);
    if (len <= UINT32_MAX)
        return msgpack_put_u8(buf, 0xdd) || msgpack_put_u32(buf, (uint32_t)len);
    celix_err_push("MessagePack array too large");
    return 1;
}

static int msgpack_pack_map_header(celix_msgpack_buffer_t* buf, size_t len) {
    if (len <= 15)
        return msgpack_put_u8(buf, (uint8_t)(0x80u | (uint8_t)len));
    if (len <= UINT16_MAX)
        return msgpack_put_u8(buf, 0xde) || msgpack_put_u16(buf, (uint16_t)len);
    if (len <= UINT32_MAX)
        return msgpack_put_u8(buf, 0xdf) || msgpack_put_u32(buf, (uint32_t)len);
    celix_err_push("MessagePack map too large");
    return 1;
}

static int msgpack_pack_unsigned(celix_msgpack_buffer_t* buf, uint64_t value) {
    if (value <= 0x7f)
        return msgpack_put_u8(buf, (uint8_t)value);
    if (value <= UINT8_MAX)
        return msgpack_put_u8(buf, 0xcc) || msgpack_put_u8(buf, (uint8_t)value);
    if (value <= UINT16_MAX)
        return msgpack_put_u8(buf, 0xcd) || msgpack_put_u16(buf, (uint16_t)value);
    if (value <= UINT32_MAX)
        return msgpack_put_u8(buf, 0xce) || msgpack_put_u32(buf, (uint32_t)value);
    return msgpack_put_u8(buf, 0xcf) || msgpack_put_u64(buf, value);
}

static int msgpack_pack_signed(celix_msgpack_buffer_t* buf, int64_t value) {
    if (value >= 0)
        return msgpack_pack_unsigned(buf, (uint64_t)value);
    if (value >= -32)
        return msgpack_put_u8(buf, (uint8_t)(int8_t)value);
    if (value >= INT8_MIN)
        return msgpack_put_u8(buf, 0xd0) || msgpack_put_u8(buf, (uint8_t)(int8_t)value);
    if (value >= INT16_MIN)
        return msgpack_put_u8(buf, 0xd1) || msgpack_put_u16(buf, (uint16_t)(int16_t)value);
    if (value >= INT32_MIN)
        return msgpack_put_u8(buf, 0xd2) || msgpack_put_u32(buf, (uint32_t)(int32_t)value);
    return msgpack_put_u8(buf, 0xd3) || msgpack_put_u64(buf, (uint64_t)value);
}

static int msgpack_pack_enum(celix_msgpack_buffer_t* buf, const dyn_type* type, int32_t value) {
    char valueStr[32];
    snprintf(valueStr, sizeof(valueStr), "%d", value);
    const struct meta_properties_head* entries = dynType_metaEntries(type);
    struct meta_entry* entry = NULL;
    TAILQ_FOREACH(entry, entries, entries) {
        if (strcmp(valueStr, entry->value) == 0) {
            return msgpack_pack_string(buf, entry->name, strlen(entry->name));
        }
    }
    celix_err_pushf("Could not find Enum value %s in enum type", valueStr);
    return 1;
}

static int msgpack_pack_dfi(celix_msgpack_buffer_t* buf, const dyn_type* type, const void* input, unsigned depth) {
    if (depth > CELIX_MSGPACK_MAX_DEPTH) {
        celix_err_push("MessagePack nesting too deep");
        return 1;
    }
    type = dynType_realType(type);
    switch (dynType_descriptorType(type)) {
    case 'Z':
        return msgpack_put_u8(buf, *(const bool*)input ? 0xc3 : 0xc2);
    case 'B':
        return msgpack_pack_signed(buf, *(const char*)input);
    case 'S':
        return msgpack_pack_signed(buf, *(const int16_t*)input);
    case 'I':
        return msgpack_pack_signed(buf, *(const int32_t*)input);
    case 'J':
        return msgpack_pack_signed(buf, *(const int64_t*)input);
    case 'N':
        return msgpack_pack_signed(buf, *(const int*)input);
    case 'b':
        return msgpack_pack_unsigned(buf, *(const uint8_t*)input);
    case 's':
        return msgpack_pack_unsigned(buf, *(const uint16_t*)input);
    case 'i':
        return msgpack_pack_unsigned(buf, *(const uint32_t*)input);
    case 'j':
        return msgpack_pack_unsigned(buf, *(const uint64_t*)input);
    case 'F': {
        uint32_t bits = 0;
        float value = *(const float*)input;
        memcpy(&bits, &value, sizeof(bits));
        return msgpack_put_u8(buf, 0xca) || msgpack_put_u32(buf, bits);
    }
    case 'D': {
        uint64_t bits = 0;
        double value = *(const double*)input;
        memcpy(&bits, &value, sizeof(bits));
        return msgpack_put_u8(buf, 0xcb) || msgpack_put_u64(buf, bits);
    }
    case 't': {
        const char* value = *(const char* const*)input;
        return value == NULL ? msgpack_put_u8(buf, 0xc0) : msgpack_pack_string(buf, value, strlen(value));
    }
    case 'E':
        return msgpack_pack_enum(buf, type, *(const int32_t*)input);
    case '{': {
        size_t count = dynType_complex_nrOfEntries(type);
        int status = msgpack_pack_map_header(buf, count);
        const struct complex_type_entries_head* entries = dynType_complex_entries(type);
        struct complex_type_entry* entry = NULL;
        int index = 0;
        TAILQ_FOREACH(entry, entries, entries) {
            if (status != 0)
                break;
            if (entry->name == NULL) {
                celix_err_push("Unamed field unsupported");
                return 1;
            }
            status = msgpack_pack_string(buf, entry->name, strlen(entry->name));
            if (status == 0) {
                const dyn_type* fieldType = dynType_complex_dynTypeAt(type, index);
                const void* fieldLoc = dynType_complex_valLocAt(type, index, (void*)input);
                status = msgpack_pack_dfi(buf, fieldType, fieldLoc, depth + 1);
            }
            ++index;
        }
        return status;
    }
    case '[': {
        uint32_t len = dynType_sequence_length(input);
        int status = msgpack_pack_array_header(buf, len);
        const dyn_type* itemType = dynType_sequence_itemType(type);
        for (uint32_t i = 0; status == 0 && i < len; ++i) {
            void* itemLoc = NULL;
            status = dynType_sequence_locForIndex(type, input, i, &itemLoc);
            if (status == 0)
                status = msgpack_pack_dfi(buf, itemType, itemLoc, depth + 1);
        }
        return status;
    }
    case '*': {
        const dyn_type* subType = dynType_typedPointer_getTypedType(type);
        if (dynType_ffiType(subType) == &ffi_type_pointer) {
            celix_err_push("Error cannot serialize pointer to pointer");
            return 1;
        }
        const void* value = *(const void* const*)input;
        return value == NULL ? msgpack_put_u8(buf, 0xc0) : msgpack_pack_dfi(buf, subType, value, depth + 1);
    }
    case 'p':
    case 'a': {
        json_t* root = NULL;
        if (jsonSerializer_serializeJson(type, input, &root) != 0)
            return 1;
        int status = msgpack_pack_json(buf, root, depth + 1);
        json_decref(root);
        return status;
    }
    default:
        celix_err_pushf("Unsupported descriptor '%c' for MessagePack", dynType_descriptorType(type));
        return 1;
    }
}

typedef struct celix_msgpack_integer {
    bool negative;
    int64_t signedValue;
    uint64_t unsignedValue;
} celix_msgpack_integer_t;

static int msgpack_peek_u8(const celix_msgpack_cursor_t* cur, uint8_t* out) {
    if (cur->pos >= cur->size) {
        celix_err_push("Truncated MessagePack input");
        return 1;
    }
    *out = cur->data[cur->pos];
    return 0;
}

static int msgpack_read_integer(celix_msgpack_cursor_t* cur, celix_msgpack_integer_t* out) {
    uint8_t tag = 0;
    if (msgpack_get_u8(cur, &tag) != 0)
        return 1;
    out->negative = false;
    out->signedValue = 0;
    out->unsignedValue = 0;
    if (tag <= 0x7f) {
        out->unsignedValue = tag;
        return 0;
    }
    if (tag >= 0xe0) {
        out->negative = true;
        out->signedValue = (int8_t)tag;
        return 0;
    }
    switch (tag) {
    case 0xcc: {
        uint8_t v;
        if (msgpack_get_u8(cur, &v))
            return 1;
        out->unsignedValue = v;
        return 0;
    }
    case 0xcd: {
        uint16_t v;
        if (msgpack_get_u16(cur, &v))
            return 1;
        out->unsignedValue = v;
        return 0;
    }
    case 0xce: {
        uint32_t v;
        if (msgpack_get_u32(cur, &v))
            return 1;
        out->unsignedValue = v;
        return 0;
    }
    case 0xcf: {
        uint64_t v;
        if (msgpack_get_u64(cur, &v))
            return 1;
        out->unsignedValue = v;
        return 0;
    }
    case 0xd0: {
        uint8_t v;
        if (msgpack_get_u8(cur, &v))
            return 1;
        int8_t s = (int8_t)v;
        if (s < 0) {
            out->negative = true;
            out->signedValue = s;
        } else
            out->unsignedValue = (uint8_t)s;
        return 0;
    }
    case 0xd1: {
        uint16_t v;
        if (msgpack_get_u16(cur, &v))
            return 1;
        int16_t s = (int16_t)v;
        if (s < 0) {
            out->negative = true;
            out->signedValue = s;
        } else
            out->unsignedValue = (uint16_t)s;
        return 0;
    }
    case 0xd2: {
        uint32_t v;
        if (msgpack_get_u32(cur, &v))
            return 1;
        int32_t s = (int32_t)v;
        if (s < 0) {
            out->negative = true;
            out->signedValue = s;
        } else
            out->unsignedValue = (uint32_t)s;
        return 0;
    }
    case 0xd3: {
        uint64_t v;
        if (msgpack_get_u64(cur, &v))
            return 1;
        int64_t s = (int64_t)v;
        if (s < 0) {
            out->negative = true;
            out->signedValue = s;
        } else
            out->unsignedValue = (uint64_t)s;
        return 0;
    }
    default:
        celix_err_pushf("Expected MessagePack integer but got tag 0x%02x", tag);
        return 1;
    }
}

static int msgpack_read_signed(celix_msgpack_cursor_t* cur, int64_t min, int64_t max, int64_t* out) {
    celix_msgpack_integer_t value;
    if (msgpack_read_integer(cur, &value) != 0)
        return 1;
    if (value.negative) {
        if (value.signedValue < min) {
            celix_err_push("MessagePack signed integer out of range");
            return 1;
        }
        *out = value.signedValue;
    } else {
        if (value.unsignedValue > (uint64_t)max) {
            celix_err_push("MessagePack signed integer out of range");
            return 1;
        }
        *out = (int64_t)value.unsignedValue;
    }
    return 0;
}

static int msgpack_read_unsigned(celix_msgpack_cursor_t* cur, uint64_t max, uint64_t* out) {
    celix_msgpack_integer_t value;
    if (msgpack_read_integer(cur, &value) != 0)
        return 1;
    if (value.negative || value.unsignedValue > max) {
        celix_err_push("MessagePack unsigned integer out of range");
        return 1;
    }
    *out = value.unsignedValue;
    return 0;
}

static int msgpack_read_string_view(celix_msgpack_cursor_t* cur, const uint8_t** data, uint32_t* len) {
    uint8_t tag = 0;
    if (msgpack_get_u8(cur, &tag) != 0)
        return 1;
    uint32_t size = 0;
    if ((tag & 0xe0u) == 0xa0u)
        size = tag & 0x1fu;
    else if (tag == 0xd9) {
        uint8_t v;
        if (msgpack_get_u8(cur, &v))
            return 1;
        size = v;
    } else if (tag == 0xda) {
        uint16_t v;
        if (msgpack_get_u16(cur, &v))
            return 1;
        size = v;
    } else if (tag == 0xdb) {
        if (msgpack_get_u32(cur, &size))
            return 1;
    } else {
        celix_err_pushf("Expected MessagePack string but got tag 0x%02x", tag);
        return 1;
    }
    const uint8_t* value = msgpack_take(cur, size);
    if (value == NULL)
        return 1;
    *data = value;
    *len = size;
    return 0;
}

static int msgpack_read_array_header(celix_msgpack_cursor_t* cur, uint32_t* len) {
    uint8_t tag = 0;
    if (msgpack_get_u8(cur, &tag) != 0)
        return 1;
    if ((tag & 0xf0u) == 0x90u) {
        *len = tag & 0x0fu;
        return 0;
    }
    if (tag == 0xdc) {
        uint16_t v;
        if (msgpack_get_u16(cur, &v))
            return 1;
        *len = v;
        return 0;
    }
    if (tag == 0xdd)
        return msgpack_get_u32(cur, len);
    celix_err_pushf("Expected MessagePack array but got tag 0x%02x", tag);
    return 1;
}

static int msgpack_read_map_header(celix_msgpack_cursor_t* cur, uint32_t* len) {
    uint8_t tag = 0;
    if (msgpack_get_u8(cur, &tag) != 0)
        return 1;
    if ((tag & 0xf0u) == 0x80u) {
        *len = tag & 0x0fu;
        return 0;
    }
    if (tag == 0xde) {
        uint16_t v;
        if (msgpack_get_u16(cur, &v))
            return 1;
        *len = v;
        return 0;
    }
    if (tag == 0xdf)
        return msgpack_get_u32(cur, len);
    celix_err_pushf("Expected MessagePack map but got tag 0x%02x", tag);
    return 1;
}

static int msgpack_skip_value(celix_msgpack_cursor_t* cur, unsigned depth) {
    if (depth > CELIX_MSGPACK_MAX_DEPTH) {
        celix_err_push("MessagePack nesting too deep");
        return 1;
    }

    uint8_t tag = 0;
    if (msgpack_get_u8(cur, &tag) != 0) {
        return 1;
    }

    if (tag <= 0x7f || tag >= 0xe0) {
        return 0; // positive/negative fixint
    }
    if ((tag & 0xe0u) == 0xa0u) {
        return msgpack_take(cur, tag & 0x1fu) == NULL ? 1 : 0; // fixstr
    }
    if ((tag & 0xf0u) == 0x90u) {
        uint32_t len = tag & 0x0fu;
        for (uint32_t i = 0; i < len; ++i) {
            if (msgpack_skip_value(cur, depth + 1) != 0) {
                return 1;
            }
        }
        return 0;
    }
    if ((tag & 0xf0u) == 0x80u) {
        uint32_t len = tag & 0x0fu;
        for (uint32_t i = 0; i < len; ++i) {
            if (msgpack_skip_value(cur, depth + 1) != 0 || msgpack_skip_value(cur, depth + 1) != 0) {
                return 1;
            }
        }
        return 0;
    }

    uint32_t len32 = 0;
    uint16_t len16 = 0;
    uint8_t len8 = 0;
    switch (tag) {
    case 0xc0: // nil
    case 0xc2: // false
    case 0xc3: // true
        return 0;
    case 0xc4: // bin8
        if (msgpack_get_u8(cur, &len8) != 0)
            return 1;
        return msgpack_take(cur, len8) == NULL ? 1 : 0;
    case 0xc5: // bin16
        if (msgpack_get_u16(cur, &len16) != 0)
            return 1;
        return msgpack_take(cur, len16) == NULL ? 1 : 0;
    case 0xc6: // bin32
        if (msgpack_get_u32(cur, &len32) != 0)
            return 1;
        return msgpack_take(cur, len32) == NULL ? 1 : 0;
    case 0xc7: // ext8
        if (msgpack_get_u8(cur, &len8) != 0 || msgpack_take(cur, 1) == NULL)
            return 1;
        return msgpack_take(cur, len8) == NULL ? 1 : 0;
    case 0xc8: // ext16
        if (msgpack_get_u16(cur, &len16) != 0 || msgpack_take(cur, 1) == NULL)
            return 1;
        return msgpack_take(cur, len16) == NULL ? 1 : 0;
    case 0xc9: // ext32
        if (msgpack_get_u32(cur, &len32) != 0 || msgpack_take(cur, 1) == NULL)
            return 1;
        return msgpack_take(cur, len32) == NULL ? 1 : 0;
    case 0xca:
        return msgpack_take(cur, 4) == NULL ? 1 : 0; // float32
    case 0xcb:
        return msgpack_take(cur, 8) == NULL ? 1 : 0; // float64
    case 0xcc:
    case 0xd0:
        return msgpack_take(cur, 1) == NULL ? 1 : 0;
    case 0xcd:
    case 0xd1:
        return msgpack_take(cur, 2) == NULL ? 1 : 0;
    case 0xce:
    case 0xd2:
        return msgpack_take(cur, 4) == NULL ? 1 : 0;
    case 0xcf:
    case 0xd3:
        return msgpack_take(cur, 8) == NULL ? 1 : 0;
    case 0xd4:
        return msgpack_take(cur, 2) == NULL ? 1 : 0; // fixext1: type + payload
    case 0xd5:
        return msgpack_take(cur, 3) == NULL ? 1 : 0; // fixext2
    case 0xd6:
        return msgpack_take(cur, 5) == NULL ? 1 : 0; // fixext4
    case 0xd7:
        return msgpack_take(cur, 9) == NULL ? 1 : 0; // fixext8
    case 0xd8:
        return msgpack_take(cur, 17) == NULL ? 1 : 0; // fixext16
    case 0xd9:                                        // str8
        if (msgpack_get_u8(cur, &len8) != 0)
            return 1;
        return msgpack_take(cur, len8) == NULL ? 1 : 0;
    case 0xda: // str16
        if (msgpack_get_u16(cur, &len16) != 0)
            return 1;
        return msgpack_take(cur, len16) == NULL ? 1 : 0;
    case 0xdb: // str32
        if (msgpack_get_u32(cur, &len32) != 0)
            return 1;
        return msgpack_take(cur, len32) == NULL ? 1 : 0;
    case 0xdc: // array16
        if (msgpack_get_u16(cur, &len16) != 0)
            return 1;
        len32 = len16;
        break;
    case 0xdd: // array32
        if (msgpack_get_u32(cur, &len32) != 0)
            return 1;
        break;
    case 0xde: // map16
        if (msgpack_get_u16(cur, &len16) != 0)
            return 1;
        len32 = len16;
        for (uint32_t i = 0; i < len32; ++i) {
            if (msgpack_skip_value(cur, depth + 1) != 0 || msgpack_skip_value(cur, depth + 1) != 0)
                return 1;
        }
        return 0;
    case 0xdf: // map32
        if (msgpack_get_u32(cur, &len32) != 0)
            return 1;
        for (uint32_t i = 0; i < len32; ++i) {
            if (msgpack_skip_value(cur, depth + 1) != 0 || msgpack_skip_value(cur, depth + 1) != 0)
                return 1;
        }
        return 0;
    case 0xc1:
    default:
        celix_err_pushf("Unsupported MessagePack tag 0x%02x", tag);
        return 1;
    }

    for (uint32_t i = 0; i < len32; ++i) {
        if (msgpack_skip_value(cur, depth + 1) != 0) {
            return 1;
        }
    }
    return 0;
}

static int msgpack_unpack_enum(celix_msgpack_cursor_t* cur, const dyn_type* type, int32_t* out) {
    const uint8_t* data = NULL;
    uint32_t len = 0;
    if (msgpack_read_string_view(cur, &data, &len) != 0)
        return 1;
    const struct meta_properties_head* entries = dynType_metaEntries(type);
    struct meta_entry* entry = NULL;
    TAILQ_FOREACH(entry, entries, entries) {
        if (strlen(entry->name) == len && memcmp(entry->name, data, len) == 0) {
            *out = (int32_t)strtol(entry->value, NULL, 10);
            return 0;
        }
    }
    celix_err_push("Could not find MessagePack enum name in enum type");
    return 1;
}

static int msgpack_unpack_builtin(celix_msgpack_cursor_t* cur, char descriptor, void* loc, unsigned depth) {
    uint8_t tag = 0;
    if (msgpack_peek_u8(cur, &tag) != 0)
        return 1;
    if (tag == 0xc0) {
        ++cur->pos;
        *(void**)loc = NULL;
        return 0;
    }
    json_t* root = msgpack_unpack_json(cur, depth + 1);
    if (root == NULL)
        return 1;
    char* json = json_dumps(root, JSON_COMPACT | JSON_ENCODE_ANY);
    json_decref(root);
    if (json == NULL)
        return 1;
    celix_status_t status;
    if (descriptor == 'p') {
        status = celix_properties_loadFromString(json, CELIX_PROPERTIES_DECODE_STRICT, (celix_properties_t**)loc);
    } else {
        status = celix_arrayList_loadFromString(json, CELIX_ARRAY_LIST_DECODE_STRICT, (celix_array_list_t**)loc);
    }
    free(json);
    return status == CELIX_SUCCESS ? 0 : 1;
}

static int msgpack_unpack_dfi(celix_msgpack_cursor_t* cur, const dyn_type* type, void* loc, unsigned depth) {
    if (depth > CELIX_MSGPACK_MAX_DEPTH) {
        celix_err_push("MessagePack nesting too deep");
        return 1;
    }
    type = dynType_realType(type);
    switch (dynType_descriptorType(type)) {
    case 'Z': {
        uint8_t tag = 0;
        if (msgpack_get_u8(cur, &tag))
            return 1;
        if (tag == 0xc2 || tag == 0xc3) {
            *(bool*)loc = tag == 0xc3;
            return 0;
        }
        celix_err_push("Expected MessagePack boolean");
        return 1;
    }
    case 'B': {
        int64_t v;
        if (msgpack_read_signed(cur, CHAR_MIN, CHAR_MAX, &v))
            return 1;
        *(char*)loc = (char)v;
        return 0;
    }
    case 'S': {
        int64_t v;
        if (msgpack_read_signed(cur, INT16_MIN, INT16_MAX, &v))
            return 1;
        *(int16_t*)loc = (int16_t)v;
        return 0;
    }
    case 'I': {
        int64_t v;
        if (msgpack_read_signed(cur, INT32_MIN, INT32_MAX, &v))
            return 1;
        *(int32_t*)loc = (int32_t)v;
        return 0;
    }
    case 'J': {
        int64_t v;
        if (msgpack_read_signed(cur, INT64_MIN, INT64_MAX, &v))
            return 1;
        *(int64_t*)loc = v;
        return 0;
    }
    case 'N': {
        int64_t v;
        if (msgpack_read_signed(cur, INT_MIN, INT_MAX, &v))
            return 1;
        *(int*)loc = (int)v;
        return 0;
    }
    case 'b': {
        uint64_t v;
        if (msgpack_read_unsigned(cur, UINT8_MAX, &v))
            return 1;
        *(uint8_t*)loc = (uint8_t)v;
        return 0;
    }
    case 's': {
        uint64_t v;
        if (msgpack_read_unsigned(cur, UINT16_MAX, &v))
            return 1;
        *(uint16_t*)loc = (uint16_t)v;
        return 0;
    }
    case 'i': {
        uint64_t v;
        if (msgpack_read_unsigned(cur, UINT32_MAX, &v))
            return 1;
        *(uint32_t*)loc = (uint32_t)v;
        return 0;
    }
    case 'j': {
        uint64_t v;
        if (msgpack_read_unsigned(cur, UINT64_MAX, &v))
            return 1;
        *(uint64_t*)loc = v;
        return 0;
    }
    case 'F': {
        uint8_t tag = 0;
        if (msgpack_get_u8(cur, &tag))
            return 1;
        if (tag == 0xca) {
            uint32_t bits;
            if (msgpack_get_u32(cur, &bits))
                return 1;
            memcpy(loc, &bits, sizeof(bits));
            return 0;
        }
        if (tag == 0xcb) {
            uint64_t bits;
            if (msgpack_get_u64(cur, &bits))
                return 1;
            double d;
            memcpy(&d, &bits, sizeof(d));
            *(float*)loc = (float)d;
            return 0;
        }
        celix_err_push("Expected MessagePack float");
        return 1;
    }
    case 'D': {
        uint8_t tag = 0;
        if (msgpack_get_u8(cur, &tag))
            return 1;
        if (tag == 0xcb) {
            uint64_t bits;
            if (msgpack_get_u64(cur, &bits))
                return 1;
            memcpy(loc, &bits, sizeof(bits));
            return 0;
        }
        if (tag == 0xca) {
            uint32_t bits;
            if (msgpack_get_u32(cur, &bits))
                return 1;
            float f;
            memcpy(&f, &bits, sizeof(f));
            *(double*)loc = f;
            return 0;
        }
        celix_err_push("Expected MessagePack double");
        return 1;
    }
    case 't': {
        uint8_t tag = 0;
        if (msgpack_peek_u8(cur, &tag))
            return 1;
        if (tag == 0xc0) {
            ++cur->pos;
            *(char**)loc = NULL;
            return 0;
        }
        const uint8_t* data = NULL;
        uint32_t len = 0;
        if (msgpack_read_string_view(cur, &data, &len))
            return 1;
        if (memchr(data, '\0', len) != NULL) {
            celix_err_push("Embedded NUL in MessagePack text");
            return 1;
        }
        char* tmp = malloc((size_t)len + 1);
        if (tmp == NULL) {
            celix_err_push("Cannot allocate MessagePack text");
            return 1;
        }
        memcpy(tmp, data, len);
        tmp[len] = '\0';
        int status = dynType_text_allocAndInit(type, loc, tmp);
        free(tmp);
        return status;
    }
    case 'E':
        return msgpack_unpack_enum(cur, type, (int32_t*)loc);
    case '[': {
        uint32_t len = 0;
        if (msgpack_read_array_header(cur, &len))
            return 1;
        if (dynType_sequence_alloc(type, loc, len) != 0)
            return 1;
        const dyn_type* itemType = dynType_sequence_itemType(type);
        for (uint32_t i = 0; i < len; ++i) {
            void* itemLoc = NULL;
            if (dynType_sequence_increaseLengthAndReturnLastLoc(type, loc, &itemLoc) != 0)
                return 1;
            if (msgpack_unpack_dfi(cur, itemType, itemLoc, depth + 1) != 0)
                return 1;
        }
        return 0;
    }
    case '{': {
        uint32_t len = 0;
        if (msgpack_read_map_header(cur, &len))
            return 1;
        size_t fieldCount = dynType_complex_nrOfEntries(type);
        bool* seen = fieldCount == 0 ? NULL : calloc(fieldCount, sizeof(*seen));
        if (fieldCount != 0 && seen == NULL) {
            celix_err_push("Cannot allocate MessagePack field tracker");
            return 1;
        }
        int status = 0;
        for (uint32_t i = 0; status == 0 && i < len; ++i) {
            const uint8_t* key = NULL;
            uint32_t keyLen = 0;
            status = msgpack_read_string_view(cur, &key, &keyLen);
            if (status != 0)
                break;
            int fieldIndex = -1;
            int idx = 0;
            const struct complex_type_entries_head* entries = dynType_complex_entries(type);
            struct complex_type_entry* entry = NULL;
            TAILQ_FOREACH(entry, entries, entries) {
                if (entry->name != NULL && strlen(entry->name) == keyLen && memcmp(entry->name, key, keyLen) == 0) {
                    fieldIndex = idx;
                    break;
                }
                ++idx;
            }
            if (fieldIndex < 0) {
                status = msgpack_skip_value(cur, depth + 1);
            } else if (seen[fieldIndex]) {
                celix_err_push("Duplicate MessagePack struct field");
                status = 1;
            } else {
                seen[fieldIndex] = true;
                const dyn_type* fieldType = dynType_complex_dynTypeAt(type, fieldIndex);
                void* fieldLoc = dynType_complex_valLocAt(type, fieldIndex, loc);
                status = msgpack_unpack_dfi(cur, fieldType, fieldLoc, depth + 1);
            }
        }
        for (size_t i = 0; status == 0 && i < fieldCount; ++i) {
            if (!seen[i]) {
                celix_err_push("Missing MessagePack struct field");
                status = 1;
            }
        }
        free(seen);
        return status;
    }
    case '*': {
        uint8_t tag = 0;
        if (msgpack_peek_u8(cur, &tag))
            return 1;
        if (tag == 0xc0) {
            ++cur->pos;
            *(void**)loc = NULL;
            return 0;
        }
        const dyn_type* subType = dynType_typedPointer_getTypedType(type);
        if (dynType_ffiType(subType) == &ffi_type_pointer) {
            celix_err_push("Error cannot deserialize pointer to pointer");
            return 1;
        }
        void* value = NULL;
        if (dynType_alloc(subType, &value) != 0)
            return 1;
        if (msgpack_unpack_dfi(cur, subType, value, depth + 1) != 0) {
            dynType_free(subType, value);
            return 1;
        }
        *(void**)loc = value;
        return 0;
    }
    case 'p':
        return msgpack_unpack_builtin(cur, 'p', loc, depth);
    case 'a':
        return msgpack_unpack_builtin(cur, 'a', loc, depth);
    default:
        celix_err_pushf("Unsupported descriptor '%c' for MessagePack", dynType_descriptorType(type));
        return 1;
    }
}

int msgpackSerializer_serialize(const dyn_type* type, const void* input, uint8_t** output, size_t* outputLength) {
    if (type == NULL || input == NULL || output == NULL || outputLength == NULL) {
        celix_err_push("Invalid argument to MessagePack serializer");
        return 1;
    }
    *output = NULL;
    *outputLength = 0;
    celix_msgpack_buffer_t buf = {0};
    int status = msgpack_pack_dfi(&buf, type, input, 0);
    if (status != 0) {
        free(buf.data);
        return status;
    }
    *output = buf.data;
    *outputLength = buf.size;
    return 0;
}

int msgpackSerializer_deserialize(const dyn_type* type, const uint8_t* input, size_t inputLength, void** result) {
    if (type == NULL || input == NULL || result == NULL) {
        celix_err_push("Invalid argument to MessagePack deserializer");
        return 1;
    }
    *result = NULL;
    const dyn_type* realType = dynType_realType(type);
    void* instance = NULL;
    if (dynType_alloc(realType, &instance) != 0)
        return 1;
    celix_msgpack_cursor_t cur = {.data = input, .size = inputLength, .pos = 0};
    int status = msgpack_unpack_dfi(&cur, realType, instance, 0);
    if (status == 0 && cur.pos != cur.size) {
        celix_err_push("Trailing bytes after MessagePack value");
        status = 1;
    }
    if (status != 0) {
        dynType_free(realType, instance);
        return status;
    }
    *result = instance;
    return 0;
}
