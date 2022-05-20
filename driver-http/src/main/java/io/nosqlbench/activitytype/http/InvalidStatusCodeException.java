package io.nosqlbench.activitytype.http;

/*
 * Copyright (c) 2022 nosqlbench
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.util.regex.Pattern;

public class InvalidStatusCodeException extends RuntimeException {
    private final long cycleValue;
    private final Pattern ok_status;
    private final int statusCode;

    @Override
    public String getMessage() {
        return "Server returned status code '" + statusCode + "' which did not match ok_status '" + ok_status.toString() + "'";
    }

    public InvalidStatusCodeException(long cycleValue, Pattern ok_status, int statusCode) {
        this.cycleValue = cycleValue;
        this.ok_status = ok_status;
        this.statusCode = statusCode;
    }
}
