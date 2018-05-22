// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef IMPALA_UTIL_ERROR_UTIL_INTERNAL_H
#define IMPALA_UTIL_ERROR_UTIL_INTERNAL_H

#include "gen-cpp/control_service.pb.h"

#include "util/error-util.h"

namespace impala {

/// Track log messages per error code.
typedef std::map<TErrorCode::type, ErrorLogEntryPB> ErrorLogMap;

/// Merge an error log entry for 'error_code' into ErrorLogMap 'm2'.
void MergeErrorLogEntry(const TErrorCode::type error_code,
    const ErrorLogEntryPB& entry, ErrorLogMap* m2);

/// Merge error map m1 into m2. Merging of error maps occurs when the errors from
/// multiple backends are merged into a single error map.  General log messages are
/// simply appended, specific errors are deduplicated by either appending a new
/// instance or incrementing the count of an existing one.
void MergeErrorMaps(const ErrorLogMap& m1, ErrorLogMap* m2);

/// Append an error to the error map. Performs the aggregation as follows: GENERAL errors
/// are appended to the list of GENERAL errors, to keep one item each in the map, while
/// for all other error codes only the count is incremented and only the first message
/// is kept as a sample.
void AppendError(ErrorLogMap* map, const ErrorMsg& e);

/// Helper method to print the contents of an ErrorMap to a stream.
void PrintErrorMap(std::ostream* stream, const ErrorLogMap& errors);

/// Reset all messages and count, but keep all keys to prevent sending already reported
/// general errors and counting the same non-general error multiple times.
void ClearErrorMap(ErrorLogMap& errors);

/// Return the number of errors within this error maps. General errors are counted
/// individually, while specific errors are counted once per distinct occurrence.
size_t ErrorCount(const ErrorLogMap& errors);

/// Generate a string representation of the error map. Produces the same output as
/// PrintErrorMap, but returns a string instead of using a stream.
std::string PrintErrorMapToString(const ErrorLogMap& errors);

}

#endif
