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

#ifndef CELIX_MSGPACK_SERIALIZER_H
#define CELIX_MSGPACK_SERIALIZER_H

#include <stddef.h>
#include <stdint.h>

#include "celix_dfi_export.h"
#include "dyn_type.h"
#ifdef __cplusplus
extern "C" {
#endif

/**
 * Serialize a DFI value to one MessagePack value.
 *
 * The caller owns the returned output buffer and must release it with free().
 * On failure, output is set to NULL and outputLength to 0.
 */
CELIX_DFI_EXPORT int
msgpackSerializer_serialize(const dyn_type* type, const void* input, uint8_t** output, size_t* outputLength);

/**
 * Deserialize one complete MessagePack value to a DFI value.
 *
 * Complex values are decoded by field name. Unknown fields are ignored, while
 * every field in the local DFI descriptor must be present. The caller owns a
 * successful result and must release it with dynType_free(type, result).
 */
CELIX_DFI_EXPORT int
msgpackSerializer_deserialize(const dyn_type* type, const uint8_t* input, size_t inputLength, void** result);

#ifdef __cplusplus
}
#endif

#endif /* CELIX_MSGPACK_SERIALIZER_H */
