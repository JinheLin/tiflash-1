# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

file(GLOB PROTO_FILES CONFIGURE_DEPENDS *.proto)
protobuf_generate_cpp(
        uncommitted_zone_proto_srcs uncommitted_zone_proto_hdrs
        ${PROTO_FILES}
)

add_library(UncommittedZoneProto ${uncommitted_zone_proto_srcs})
target_include_directories(UncommittedZoneProto PUBLIC ${Protobuf_INCLUDE_DIR})
target_compile_options(UncommittedZoneProto PRIVATE -Wno-unused-parameter)
