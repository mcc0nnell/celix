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
#include <msgpack.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CELIX_MSGPACK_MAX_DEPTH 64u

static bool celixMsgpack_depthExceeded(unsigned depth) {
    if (depth <= CELIX_MSGPACK_MAX_DEPTH) return false;
    celix_err_push("MessagePack nesting too deep");
    return true;
}

static int celixMsgpack_fail(const char* message) {
    celix_err_push(message);
    return 1;
}

static int celixMsgpack_packDfi(msgpack_packer* pk, const dyn_type* type, const void* input, unsigned depth);
static int celixMsgpack_unpackDfi(const msgpack_object* object, const dyn_type* type, void* loc, unsigned depth);

static int celixMsgpack_packJson(msgpack_packer* pk, const json_t* value, unsigned depth) {
    if (celixMsgpack_depthExceeded(depth)) return 1;
    if (json_is_null(value)) return msgpack_pack_nil(pk) != 0;
    if (json_is_true(value)) return msgpack_pack_true(pk) != 0;
    if (json_is_false(value)) return msgpack_pack_false(pk) != 0;
    if (json_is_integer(value)) return msgpack_pack_int64(pk, json_integer_value(value)) != 0;
    if (json_is_real(value)) return msgpack_pack_double(pk, json_real_value(value)) != 0;
    if (json_is_string(value)) return msgpack_pack_str_with_body(pk, json_string_value(value), json_string_length(value)) != 0;
    if (json_is_array(value)) {
        size_t len = json_array_size(value);
        if (msgpack_pack_array(pk, len) != 0) return 1;
        for (size_t i = 0; i < len; ++i) {
            if (celixMsgpack_packJson(pk, json_array_get(value, i), depth + 1) != 0) return 1;
        }
        return 0;
    }
    if (json_is_object(value)) {
        size_t len = json_object_size(value);
        if (msgpack_pack_map(pk, len) != 0) return 1;
        const char* key = NULL;
        json_t* entry = NULL;
        json_object_foreach((json_t*)value, key, entry) {
            if (msgpack_pack_str_with_body(pk, key, strlen(key)) != 0 || celixMsgpack_packJson(pk, entry, depth + 1) != 0) return 1;
        }
        return 0;
    }
    return celixMsgpack_fail("Unsupported JSON value for MessagePack serialization");
}

static int celixMsgpack_packEnum(msgpack_packer* pk, const dyn_type* type, int32_t value) {
    char valueStr[32];
    snprintf(valueStr, sizeof(valueStr), "%d", value);
    const struct meta_properties_head* entries = dynType_metaEntries(type);
    struct meta_entry* entry = NULL;
    TAILQ_FOREACH(entry, entries, entries) {
        if (strcmp(valueStr, entry->value) == 0) return msgpack_pack_str_with_body(pk, entry->name, strlen(entry->name)) != 0;
    }
    celix_err_pushf("Could not find Enum value %s in enum type", valueStr);
    return 1;
}

static int celixMsgpack_packDfi(msgpack_packer* pk, const dyn_type* type, const void* input, unsigned depth) {
    if (celixMsgpack_depthExceeded(depth)) return 1;
    type = dynType_realType(type);
    switch (dynType_descriptorType(type)) {
        case 'Z': return (*(const bool*)input ? msgpack_pack_true(pk) : msgpack_pack_false(pk)) != 0;
        case 'B': return msgpack_pack_int8(pk, *(const char*)input) != 0;
        case 'S': return msgpack_pack_int16(pk, *(const int16_t*)input) != 0;
        case 'I': return msgpack_pack_int32(pk, *(const int32_t*)input) != 0;
        case 'J': return msgpack_pack_int64(pk, *(const int64_t*)input) != 0;
        case 'N': return msgpack_pack_int(pk, *(const int*)input) != 0;
        case 'b': return msgpack_pack_uint8(pk, *(const uint8_t*)input) != 0;
        case 's': return msgpack_pack_uint16(pk, *(const uint16_t*)input) != 0;
        case 'i': return msgpack_pack_uint32(pk, *(const uint32_t*)input) != 0;
        case 'j': return msgpack_pack_uint64(pk, *(const uint64_t*)input) != 0;
        case 'F': return msgpack_pack_float(pk, *(const float*)input) != 0;
        case 'D': return msgpack_pack_double(pk, *(const double*)input) != 0;
        case 't': {
            const char* value = *(const char* const*)input;
            return value == NULL ? msgpack_pack_nil(pk) != 0 : msgpack_pack_str_with_body(pk, value, strlen(value)) != 0;
        }
        case 'E': return celixMsgpack_packEnum(pk, type, *(const int32_t*)input);
        case '{': {
            size_t count = dynType_complex_nrOfEntries(type);
            if (msgpack_pack_map(pk, count) != 0) return 1;
            const struct complex_type_entries_head* entries = dynType_complex_entries(type);
            struct complex_type_entry* entry = NULL;
            int index = 0;
            TAILQ_FOREACH(entry, entries, entries) {
                if (entry->name == NULL) return celixMsgpack_fail("Unnamed field unsupported");
                const dyn_type* fieldType = dynType_complex_dynTypeAt(type, index);
                const void* fieldLoc = dynType_complex_valLocAt(type, index, (void*)input);
                if (msgpack_pack_str_with_body(pk, entry->name, strlen(entry->name)) != 0 ||
                    celixMsgpack_packDfi(pk, fieldType, fieldLoc, depth + 1) != 0) return 1;
                ++index;
            }
            return 0;
        }
        case '[': {
            uint32_t len = dynType_sequence_length(input);
            if (msgpack_pack_array(pk, len) != 0) return 1;
            const dyn_type* itemType = dynType_sequence_itemType(type);
            for (uint32_t i = 0; i < len; ++i) {
                void* itemLoc = NULL;
                if (dynType_sequence_locForIndex(type, input, i, &itemLoc) != 0 ||
                    celixMsgpack_packDfi(pk, itemType, itemLoc, depth + 1) != 0) return 1;
            }
            return 0;
        }
        case '*': {
            const dyn_type* subType = dynType_typedPointer_getTypedType(type);
            if (dynType_ffiType(subType) == &ffi_type_pointer) return celixMsgpack_fail("Error cannot serialize pointer to pointer");
            const void* value = *(const void* const*)input;
            return value == NULL ? msgpack_pack_nil(pk) != 0 : celixMsgpack_packDfi(pk, subType, value, depth + 1);
        }
        case 'p':
        case 'a': {
            json_t* root = NULL;
            if (jsonSerializer_serializeJson(type, input, &root) != 0) return 1;
            int status = celixMsgpack_packJson(pk, root, depth + 1);
            json_decref(root);
            return status;
        }
        default:
            celix_err_pushf("Unsupported descriptor '%c' for MessagePack", dynType_descriptorType(type));
            return 1;
    }
}

static json_t* celixMsgpack_objectToJson(const msgpack_object* object, unsigned depth) {
    if (celixMsgpack_depthExceeded(depth)) return NULL;
    switch (object->type) {
        case MSGPACK_OBJECT_NIL: return json_null();
        case MSGPACK_OBJECT_BOOLEAN: return json_boolean(object->via.boolean);
        case MSGPACK_OBJECT_POSITIVE_INTEGER:
            if (object->via.u64 > (uint64_t)INT64_MAX) {
                celix_err_push("MessagePack unsigned integer exceeds Celix JSON integer range");
                return NULL;
            }
            return json_integer((json_int_t)object->via.u64);
        case MSGPACK_OBJECT_NEGATIVE_INTEGER: return json_integer((json_int_t)object->via.i64);
        case MSGPACK_OBJECT_FLOAT32:
        case MSGPACK_OBJECT_FLOAT64: return json_real(object->via.f64);
        case MSGPACK_OBJECT_STR: return json_stringn(object->via.str.ptr, object->via.str.size);
        case MSGPACK_OBJECT_ARRAY: {
            json_t* array = json_array();
            if (array == NULL) return NULL;
            for (uint32_t i = 0; i < object->via.array.size; ++i) {
                json_t* item = celixMsgpack_objectToJson(&object->via.array.ptr[i], depth + 1);
                if (item == NULL || json_array_append_new(array, item) != 0) {
                    json_decref(array);
                    return NULL;
                }
            }
            return array;
        }
        case MSGPACK_OBJECT_MAP: {
            json_t* map = json_object();
            if (map == NULL) return NULL;
            for (uint32_t i = 0; i < object->via.map.size; ++i) {
                const msgpack_object_kv* kv = &object->via.map.ptr[i];
                if (kv->key.type != MSGPACK_OBJECT_STR || memchr(kv->key.via.str.ptr, '\0', kv->key.via.str.size) != NULL) {
                    celix_err_push("MessagePack map key must be a string");
                    json_decref(map);
                    return NULL;
                }
                char* key = strndup(kv->key.via.str.ptr, kv->key.via.str.size);
                if (key == NULL) { json_decref(map); return NULL; }
                json_t* value = celixMsgpack_objectToJson(&kv->val, depth + 1);
                if (value == NULL || json_object_set_new(map, key, value) != 0) {
                    free(key);
                    json_decref(map);
                    return NULL;
                }
                free(key);
            }
            return map;
        }
        case MSGPACK_OBJECT_BIN:
        case MSGPACK_OBJECT_EXT:
        default:
            celix_err_push("Unsupported MessagePack value for Celix JSON conversion");
            return NULL;
    }
}

static int celixMsgpack_readSigned(const msgpack_object* object, int64_t minimum, int64_t maximum, int64_t* out) {
    int64_t value;
    if (object->type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
        value = object->via.i64;
    } else if (object->type == MSGPACK_OBJECT_POSITIVE_INTEGER && object->via.u64 <= (uint64_t)INT64_MAX) {
        value = (int64_t)object->via.u64;
    } else {
        return celixMsgpack_fail("Expected MessagePack integer in signed range");
    }
    if (value < minimum || value > maximum) return celixMsgpack_fail("MessagePack integer out of range");
    *out = value;
    return 0;
}

static int celixMsgpack_readUnsigned(const msgpack_object* object, uint64_t maximum, uint64_t* out) {
    if (object->type != MSGPACK_OBJECT_POSITIVE_INTEGER || object->via.u64 > maximum)
        return celixMsgpack_fail("Expected MessagePack integer in unsigned range");
    *out = object->via.u64;
    return 0;
}

static int celixMsgpack_unpackEnum(const msgpack_object* object, const dyn_type* type, int32_t* out) {
    if (object->type != MSGPACK_OBJECT_STR) return celixMsgpack_fail("Expected MessagePack string for enum");
    const struct meta_properties_head* entries = dynType_metaEntries(type);
    struct meta_entry* entry = NULL;
    TAILQ_FOREACH(entry, entries, entries) {
        if (strlen(entry->name) == object->via.str.size && memcmp(entry->name, object->via.str.ptr, object->via.str.size) == 0) {
            *out = (int32_t)strtol(entry->value, NULL, 10);
            return 0;
        }
    }
    return celixMsgpack_fail("Could not find MessagePack enum name in enum type");
}

static int celixMsgpack_unpackBuiltin(const msgpack_object* object, char descriptor, void* loc, unsigned depth) {
    if (object->type == MSGPACK_OBJECT_NIL) {
        *(void**)loc = NULL;
        return 0;
    }
    json_t* root = celixMsgpack_objectToJson(object, depth + 1);
    if (root == NULL) return 1;
    char* json = json_dumps(root, JSON_COMPACT | JSON_ENCODE_ANY);
    json_decref(root);
    if (json == NULL) return 1;
    celix_status_t status = descriptor == 'p'
            ? celix_properties_loadFromString(json, CELIX_PROPERTIES_DECODE_STRICT, (celix_properties_t**)loc)
            : celix_arrayList_loadFromString(json, CELIX_ARRAY_LIST_DECODE_STRICT, (celix_array_list_t**)loc);
    free(json);
    return status == CELIX_SUCCESS ? 0 : 1;
}

static int celixMsgpack_findField(const dyn_type* type, const msgpack_object* key) {
    if (key->type != MSGPACK_OBJECT_STR) return -2;
    int index = 0;
    const struct complex_type_entries_head* entries = dynType_complex_entries(type);
    struct complex_type_entry* entry = NULL;
    TAILQ_FOREACH(entry, entries, entries) {
        if (entry->name != NULL && strlen(entry->name) == key->via.str.size && memcmp(entry->name, key->via.str.ptr, key->via.str.size) == 0) return index;
        ++index;
    }
    return -1;
}

static int celixMsgpack_unpackDfi(const msgpack_object* object, const dyn_type* type, void* loc, unsigned depth) {
    if (celixMsgpack_depthExceeded(depth)) return 1;
    type = dynType_realType(type);
    switch (dynType_descriptorType(type)) {
        case 'Z':
            if (object->type != MSGPACK_OBJECT_BOOLEAN) return celixMsgpack_fail("Expected MessagePack boolean");
            *(bool*)loc = object->via.boolean;
            return 0;
        case 'B': { int64_t v; if (celixMsgpack_readSigned(object, CHAR_MIN, CHAR_MAX, &v)) return 1; *(char*)loc = (char)v; return 0; }
        case 'S': { int64_t v; if (celixMsgpack_readSigned(object, INT16_MIN, INT16_MAX, &v)) return 1; *(int16_t*)loc = (int16_t)v; return 0; }
        case 'I': { int64_t v; if (celixMsgpack_readSigned(object, INT32_MIN, INT32_MAX, &v)) return 1; *(int32_t*)loc = (int32_t)v; return 0; }
        case 'J': { int64_t v; if (celixMsgpack_readSigned(object, INT64_MIN, INT64_MAX, &v)) return 1; *(int64_t*)loc = v; return 0; }
        case 'N': { int64_t v; if (celixMsgpack_readSigned(object, INT_MIN, INT_MAX, &v)) return 1; *(int*)loc = (int)v; return 0; }
        case 'b': { uint64_t v; if (celixMsgpack_readUnsigned(object, UINT8_MAX, &v)) return 1; *(uint8_t*)loc = (uint8_t)v; return 0; }
        case 's': { uint64_t v; if (celixMsgpack_readUnsigned(object, UINT16_MAX, &v)) return 1; *(uint16_t*)loc = (uint16_t)v; return 0; }
        case 'i': { uint64_t v; if (celixMsgpack_readUnsigned(object, UINT32_MAX, &v)) return 1; *(uint32_t*)loc = (uint32_t)v; return 0; }
        case 'j': { uint64_t v; if (celixMsgpack_readUnsigned(object, UINT64_MAX, &v)) return 1; *(uint64_t*)loc = v; return 0; }
        case 'F':
            if (object->type != MSGPACK_OBJECT_FLOAT32 && object->type != MSGPACK_OBJECT_FLOAT64) return celixMsgpack_fail("Expected MessagePack float");
            *(float*)loc = (float)object->via.f64;
            return 0;
        case 'D':
            if (object->type != MSGPACK_OBJECT_FLOAT32 && object->type != MSGPACK_OBJECT_FLOAT64) return celixMsgpack_fail("Expected MessagePack double");
            *(double*)loc = object->via.f64;
            return 0;
        case 't': {
            if (object->type == MSGPACK_OBJECT_NIL) { *(char**)loc = NULL; return 0; }
            if (object->type != MSGPACK_OBJECT_STR) return celixMsgpack_fail("Expected MessagePack string");
            if (memchr(object->via.str.ptr, '\0', object->via.str.size) != NULL) return celixMsgpack_fail("Embedded NUL in MessagePack text");
            char* value = strndup(object->via.str.ptr, object->via.str.size);
            if (value == NULL) return celixMsgpack_fail("Cannot allocate MessagePack text");
            int status = dynType_text_allocAndInit(type, loc, value);
            free(value);
            return status;
        }
        case 'E': return celixMsgpack_unpackEnum(object, type, (int32_t*)loc);
        case '[': {
            if (object->type != MSGPACK_OBJECT_ARRAY) return celixMsgpack_fail("Expected MessagePack array");
            if (dynType_sequence_alloc(type, loc, object->via.array.size) != 0) return 1;
            const dyn_type* itemType = dynType_sequence_itemType(type);
            for (uint32_t i = 0; i < object->via.array.size; ++i) {
                void* itemLoc = NULL;
                if (dynType_sequence_increaseLengthAndReturnLastLoc(type, loc, &itemLoc) != 0 ||
                    celixMsgpack_unpackDfi(&object->via.array.ptr[i], itemType, itemLoc, depth + 1) != 0) return 1;
            }
            return 0;
        }
        case '{': {
            if (object->type != MSGPACK_OBJECT_MAP) return celixMsgpack_fail("Expected MessagePack map");
            size_t fieldCount = dynType_complex_nrOfEntries(type);
            bool* seen = fieldCount == 0 ? NULL : calloc(fieldCount, sizeof(*seen));
            if (fieldCount != 0 && seen == NULL) return celixMsgpack_fail("Cannot allocate MessagePack field tracker");
            int status = 0;
            for (uint32_t i = 0; status == 0 && i < object->via.map.size; ++i) {
                const msgpack_object_kv* kv = &object->via.map.ptr[i];
                int fieldIndex = celixMsgpack_findField(type, &kv->key);
                if (fieldIndex == -2) {
                    status = celixMsgpack_fail("MessagePack struct field name must be a string");
                } else if (fieldIndex >= 0) {
                    if (seen[fieldIndex]) {
                        status = celixMsgpack_fail("Duplicate MessagePack struct field");
                    } else {
                        seen[fieldIndex] = true;
                        const dyn_type* fieldType = dynType_complex_dynTypeAt(type, fieldIndex);
                        void* fieldLoc = dynType_complex_valLocAt(type, fieldIndex, loc);
                        status = celixMsgpack_unpackDfi(&kv->val, fieldType, fieldLoc, depth + 1);
                    }
                }
            }
            for (size_t i = 0; status == 0 && i < fieldCount; ++i) {
                if (!seen[i]) status = celixMsgpack_fail("Missing MessagePack struct field");
            }
            free(seen);
            return status;
        }
        case '*': {
            if (object->type == MSGPACK_OBJECT_NIL) { *(void**)loc = NULL; return 0; }
            const dyn_type* subType = dynType_typedPointer_getTypedType(type);
            if (dynType_ffiType(subType) == &ffi_type_pointer) return celixMsgpack_fail("Error cannot deserialize pointer to pointer");
            void* value = NULL;
            if (dynType_alloc(subType, &value) != 0) return 1;
            if (celixMsgpack_unpackDfi(object, subType, value, depth + 1) != 0) { dynType_free(subType, value); return 1; }
            *(void**)loc = value;
            return 0;
        }
        case 'p': return celixMsgpack_unpackBuiltin(object, 'p', loc, depth);
        case 'a': return celixMsgpack_unpackBuiltin(object, 'a', loc, depth);
        default:
            celix_err_pushf("Unsupported descriptor '%c' for MessagePack", dynType_descriptorType(type));
            return 1;
    }
}

int msgpackSerializer_serialize(const dyn_type* type, const void* input, uint8_t** output, size_t* outputLength) {
    if (type == NULL || input == NULL || output == NULL || outputLength == NULL)
        return celixMsgpack_fail("Invalid argument to MessagePack serializer");
    *output = NULL;
    *outputLength = 0;
    msgpack_sbuffer buffer;
    msgpack_sbuffer_init(&buffer);
    msgpack_packer packer;
    msgpack_packer_init(&packer, &buffer, msgpack_sbuffer_write);
    if (celixMsgpack_packDfi(&packer, type, input, 0) != 0) {
        msgpack_sbuffer_destroy(&buffer);
        return 1;
    }
    *outputLength = buffer.size;
    *output = (uint8_t*)msgpack_sbuffer_release(&buffer);
    return 0;
}

int msgpackSerializer_deserialize(const dyn_type* type, const uint8_t* input, size_t inputLength, void** result) {
    if (type == NULL || input == NULL || result == NULL)
        return celixMsgpack_fail("Invalid argument to MessagePack deserializer");
    *result = NULL;
    msgpack_unpacked unpacked;
    msgpack_unpacked_init(&unpacked);
    size_t offset = 0;
    msgpack_unpack_return unpackStatus = msgpack_unpack_next(&unpacked, (const char*)input, inputLength, &offset);
    if (unpackStatus != MSGPACK_UNPACK_SUCCESS || offset != inputLength) {
        celix_err_push(unpackStatus == MSGPACK_UNPACK_EXTRA_BYTES || offset != inputLength
                ? "Trailing bytes after MessagePack value"
                : "Invalid or truncated MessagePack input");
        msgpack_unpacked_destroy(&unpacked);
        return 1;
    }
    const dyn_type* realType = dynType_realType(type);
    void* instance = NULL;
    if (dynType_alloc(realType, &instance) != 0) {
        msgpack_unpacked_destroy(&unpacked);
        return 1;
    }
    int status = celixMsgpack_unpackDfi(&unpacked.data, realType, instance, 0);
    msgpack_unpacked_destroy(&unpacked);
    if (status != 0) {
        dynType_free(realType, instance);
        return status;
    }
    *result = instance;
    return 0;
}
