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
include_directories(SYSTEM velox/external)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

add_library(velox_external_date tz.cpp)

      target_compile_definitions(velox_external_date PRIVATE AUTO_DOWNLOAD=1 HAS_REMOTE_API=1 )
      

find_package( CURL REQUIRED )
        target_include_directories( velox_external_date SYSTEM PRIVATE ${CURL_INCLUDE_DIRS} )
        target_link_libraries( velox_external_date PRIVATE ${CURL_LIBRARIES} )

