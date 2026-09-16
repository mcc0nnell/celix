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

#include <gtest/gtest.h>

#include "celix_array_list.h"
#include "celix_properties.h"
#include "dyn_type.h"
#include "msgpack_serializer.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>

class MsgpackSerializerTestSuite : public ::testing::Test {};
TEST_F(MsgpackSerializerTestSuite, SerializesCanonicalIntegers) {
    dyn_type* type = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("I", nullptr, nullptr, &type));

    int32_t value = 42;
    uint8_t* output = nullptr;
    size_t outputLength = 0;
    ASSERT_EQ(0, msgpackSerializer_serialize(type, &value, &output, &outputLength));
    ASSERT_EQ(1, outputLength);
    EXPECT_EQ(0x2a, output[0]);
    free(output);

    value = -33;
    output = nullptr;
    outputLength = 0;
    ASSERT_EQ(0, msgpackSerializer_serialize(type, &value, &output, &outputLength));
    ASSERT_EQ(2, outputLength);
    EXPECT_EQ(0xd0, output[0]);
    EXPECT_EQ(0xdf, output[1]);
    free(output);

    dynType_destroy(type);
}
TEST_F(MsgpackSerializerTestSuite, DeserializesInteger) {
    dyn_type* type = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("I", nullptr, nullptr, &type));

    const uint8_t input[] = {0xd0, 0xdf};
    void* result = nullptr;
    ASSERT_EQ(0, msgpackSerializer_deserialize(type, input, sizeof(input), &result));
    ASSERT_NE(nullptr, result);
    EXPECT_EQ(-33, *static_cast<int32_t*>(result));

    dynType_free(type, result);
    dynType_destroy(type);
}

struct msgpack_example {
    struct {
        uint32_t cap;
        uint32_t len;
        int32_t* buf;
    } numbers;
    const char* name;
};
TEST_F(MsgpackSerializerTestSuite, RoundTripsComplexType) {
    dyn_type* type = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("{[It numbers name}", nullptr, nullptr, &type));

    int32_t values[] = {22, 32, 42};
    msgpack_example input{};
    input.numbers.cap = 3;
    input.numbers.len = 3;
    input.numbers.buf = values;
    input.name = "edge-node";

    uint8_t* output = nullptr;
    size_t outputLength = 0;
    ASSERT_EQ(0, msgpackSerializer_serialize(type, &input, &output, &outputLength));
    ASSERT_GT(outputLength, 0);
    EXPECT_EQ(0x82, output[0]);

    void* result = nullptr;
    ASSERT_EQ(0, msgpackSerializer_deserialize(type, output, outputLength, &result));
    auto* decoded = static_cast<msgpack_example*>(result);
    ASSERT_NE(nullptr, decoded);
    ASSERT_EQ(3, decoded->numbers.len);
    EXPECT_EQ(22, decoded->numbers.buf[0]);
    EXPECT_EQ(32, decoded->numbers.buf[1]);
    EXPECT_EQ(42, decoded->numbers.buf[2]);
    ASSERT_NE(nullptr, decoded->name);
    EXPECT_STREQ("edge-node", decoded->name);
    free(output);
    dynType_free(type, result);
    dynType_destroy(type);
}

TEST_F(MsgpackSerializerTestSuite, RejectsTrailingBytes) {
    dyn_type* type = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("I", nullptr, nullptr, &type));

    const uint8_t input[] = {0x2a, 0x00};
    void* result = nullptr;
    EXPECT_NE(0, msgpackSerializer_deserialize(type, input, sizeof(input), &result));
    EXPECT_EQ(nullptr, result);

    dynType_destroy(type);
}

TEST_F(MsgpackSerializerTestSuite, RoundTripsFullUint64Range) {
    dyn_type* type = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("j", nullptr, nullptr, &type));
    uint64_t value = UINT64_MAX;
    uint8_t* output = nullptr;
    size_t outputLength = 0;
    ASSERT_EQ(0, msgpackSerializer_serialize(type, &value, &output, &outputLength));
    ASSERT_EQ(9, outputLength);
    EXPECT_EQ(0xcf, output[0]);
    void* result = nullptr;
    ASSERT_EQ(0, msgpackSerializer_deserialize(type, output, outputLength, &result));
    EXPECT_EQ(UINT64_MAX, *static_cast<uint64_t*>(result));
    free(output);
    dynType_free(type, result);
    dynType_destroy(type);
}

TEST_F(MsgpackSerializerTestSuite, UsesFloat32ForDfiFloat) {
    dyn_type* type = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("F", nullptr, nullptr, &type));
    float value = 3.25f;
    uint8_t* output = nullptr;
    size_t outputLength = 0;
    ASSERT_EQ(0, msgpackSerializer_serialize(type, &value, &output, &outputLength));
    ASSERT_EQ(5, outputLength);
    EXPECT_EQ(0xca, output[0]);
    void* result = nullptr;
    ASSERT_EQ(0, msgpackSerializer_deserialize(type, output, outputLength, &result));
    EXPECT_FLOAT_EQ(value, *static_cast<float*>(result));
    free(output);
    dynType_free(type, result);
    dynType_destroy(type);
}

TEST_F(MsgpackSerializerTestSuite, IgnoresUnknownStructFields) {
    dyn_type* type = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("{I value}", nullptr, nullptr, &type));
    const uint8_t input[] = {
        0x82, 0xa5, 'v', 'a', 'l', 'u', 'e', 0x2a, 0xa6, 'f', 'u', 't', 'u', 'r', 'e', 0x92, 0x01, 0x02};
    void* result = nullptr;
    ASSERT_EQ(0, msgpackSerializer_deserialize(type, input, sizeof(input), &result));
    EXPECT_EQ(42, *static_cast<int32_t*>(result));
    dynType_free(type, result);
    dynType_destroy(type);
}

TEST_F(MsgpackSerializerTestSuite, RejectsMissingAndTruncatedFields) {
    dyn_type* type = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("{It value name}", nullptr, nullptr, &type));
    const uint8_t missing[] = {0x81, 0xa5, 'v', 'a', 'l', 'u', 'e', 0x2a};
    void* result = nullptr;
    EXPECT_NE(0, msgpackSerializer_deserialize(type, missing, sizeof(missing), &result));
    EXPECT_EQ(nullptr, result);
    const uint8_t truncated[] = {0x82, 0xa5, 'v', 'a', 'l', 'u', 'e', 0x2a, 0xa4, 'n'};
    EXPECT_NE(0, msgpackSerializer_deserialize(type, truncated, sizeof(truncated), &result));
    EXPECT_EQ(nullptr, result);
    dynType_destroy(type);
}

TEST_F(MsgpackSerializerTestSuite, RoundTripsEnumAndTypedPointer) {
    dyn_type* enumType = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("#v1=1;#v2=2;E", nullptr, nullptr, &enumType));
    int32_t enumValue = 2;
    uint8_t* output = nullptr;
    size_t outputLength = 0;
    ASSERT_EQ(0, msgpackSerializer_serialize(enumType, &enumValue, &output, &outputLength));
    ASSERT_EQ(3, outputLength);
    EXPECT_EQ(0xa2, output[0]);
    EXPECT_EQ('v', output[1]);
    EXPECT_EQ('2', output[2]);
    void* enumResult = nullptr;
    ASSERT_EQ(0, msgpackSerializer_deserialize(enumType, output, outputLength, &enumResult));
    EXPECT_EQ(2, *static_cast<int32_t*>(enumResult));
    free(output);
    dynType_free(enumType, enumResult);
    dynType_destroy(enumType);

    dyn_type* pointerType = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("*D", nullptr, nullptr, &pointerType));
    double pointedValue = 0.125;
    double* pointer = &pointedValue;
    output = nullptr;
    outputLength = 0;
    ASSERT_EQ(0, msgpackSerializer_serialize(pointerType, &pointer, &output, &outputLength));
    void* pointerResult = nullptr;
    ASSERT_EQ(0, msgpackSerializer_deserialize(pointerType, output, outputLength, &pointerResult));
    ASSERT_NE(nullptr, *static_cast<double**>(pointerResult));
    EXPECT_DOUBLE_EQ(pointedValue, **static_cast<double**>(pointerResult));
    free(output);
    dynType_free(pointerType, pointerResult);
    pointer = nullptr;
    ASSERT_EQ(0, msgpackSerializer_serialize(pointerType, &pointer, &output, &outputLength));
    ASSERT_EQ(1, outputLength);
    EXPECT_EQ(0xc0, output[0]);
    free(output);
    dynType_destroy(pointerType);
}

TEST_F(MsgpackSerializerTestSuite, RoundTripsPropertiesAndArrayListFallbacks) {
    dyn_type* propsType = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("p", nullptr, nullptr, &propsType));
    celix_properties_t* props = celix_properties_create();
    ASSERT_NE(nullptr, props);
    ASSERT_EQ(CELIX_SUCCESS, celix_properties_setString(props, "node", "edge"));
    ASSERT_EQ(CELIX_SUCCESS, celix_properties_setLong(props, "count", 42));
    uint8_t* output = nullptr;
    size_t outputLength = 0;
    ASSERT_EQ(0, msgpackSerializer_serialize(propsType, &props, &output, &outputLength));
    void* propsResult = nullptr;
    ASSERT_EQ(0, msgpackSerializer_deserialize(propsType, output, outputLength, &propsResult));
    auto* decodedProps = *static_cast<celix_properties_t**>(propsResult);
    ASSERT_NE(nullptr, decodedProps);
    EXPECT_STREQ("edge", celix_properties_getString(decodedProps, "node"));
    EXPECT_EQ(42, celix_properties_getLong(decodedProps, "count", -1));
    free(output);
    dynType_free(propsType, propsResult);
    celix_properties_destroy(props);
    dynType_destroy(propsType);

    dyn_type* listType = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("a", nullptr, nullptr, &listType));
    celix_array_list_t* list = celix_arrayList_createStringArray();
    ASSERT_NE(nullptr, list);
    ASSERT_EQ(CELIX_SUCCESS, celix_arrayList_addString(list, "alpha"));
    ASSERT_EQ(CELIX_SUCCESS, celix_arrayList_addString(list, "beta"));
    output = nullptr;
    outputLength = 0;
    ASSERT_EQ(0, msgpackSerializer_serialize(listType, &list, &output, &outputLength));
    void* listResult = nullptr;
    ASSERT_EQ(0, msgpackSerializer_deserialize(listType, output, outputLength, &listResult));
    auto* decodedList = *static_cast<celix_array_list_t**>(listResult);
    ASSERT_NE(nullptr, decodedList);
    ASSERT_EQ(2, celix_arrayList_size(decodedList));
    EXPECT_STREQ("alpha", celix_arrayList_getString(decodedList, 0));
    EXPECT_STREQ("beta", celix_arrayList_getString(decodedList, 1));
    free(output);
    dynType_free(listType, listResult);
    celix_arrayList_destroy(list);
    dynType_destroy(listType);
}

TEST_F(MsgpackSerializerTestSuite, SkipsUnknownBinaryAndExtensionFields) {
    dyn_type* type = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("{I value}", nullptr, nullptr, &type));
    const uint8_t input[] = {0x83, 0xa5, 'v',  'a',  'l',  'u',  'e', 0x2a, 0xa3, 'b',  'i',  'n',
                             0xc4, 0x03, 0xde, 0xad, 0xbe, 0xa3, 'e', 'x',  't',  0xd4, 0x01, 0xff};
    void* result = nullptr;
    ASSERT_EQ(0, msgpackSerializer_deserialize(type, input, sizeof(input), &result));
    EXPECT_EQ(42, *static_cast<int32_t*>(result));
    dynType_free(type, result);
    dynType_destroy(type);
}

TEST_F(MsgpackSerializerTestSuite, DeserializesStructFieldsByNameNotWireOrder) {
    dyn_type* type = nullptr;
    ASSERT_EQ(0, dynType_parseWithStr("{It value name}", nullptr, nullptr, &type));
    const uint8_t input[] = {
        0x82, 0xa4, 'n', 'a', 'm', 'e', 0xa4, 'e', 'd', 'g', 'e', 0xa5, 'v', 'a', 'l', 'u', 'e', 0x2a};
    void* result = nullptr;
    ASSERT_EQ(0, msgpackSerializer_deserialize(type, input, sizeof(input), &result));
    struct decoded_type {
        int32_t value;
        char* name;
    };
    auto* decoded = static_cast<decoded_type*>(result);
    EXPECT_EQ(42, decoded->value);
    ASSERT_NE(nullptr, decoded->name);
    EXPECT_STREQ("edge", decoded->name);
    dynType_free(type, result);
    dynType_destroy(type);
}
