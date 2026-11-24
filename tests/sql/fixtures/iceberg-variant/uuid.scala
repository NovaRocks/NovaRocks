// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

val schema=new Schema(Types.NestedField.required(1,"id",Types.IntegerType.get()),Types.NestedField.optional(2,"k",Types.UUIDType.get()))
val t=VariantFixture.create(schema)
val key = java.util.UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56")
VariantFixture.write(t, Seq(Seq[Any](1, key), Seq[Any](2, null)))
VariantFixture.write(t, Seq(Seq[Any](3, key)))
VariantFixture.remember(t)
println("IRU9_UUID_OK")
