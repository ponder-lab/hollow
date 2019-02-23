/*
 *  Copyright 2016-2019 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.hollow.core.util;

import static java.util.Objects.requireNonNull;

/**
 * Internal API.
 */
public final class Threads {
    private static final ThreadNamer NAMER = () -> "hollow";

    private Threads() {}

    public static Thread daemonThread(Runnable r, Class<?> context, String description) {
        return daemonThread(r, NAMER, context, description);
    }

    public static Thread daemonThread(Runnable r, ThreadNamer namer, Class<?> context, String description) {
        requireNonNull(r, "runnable required");
        requireNonNull(context, "context required");
        requireNonNull(description, "description required");

        Thread t = new Thread(r, namer.name(context, description));
        t.setDaemon(true);
        return t;
    }

    @FunctionalInterface
    public interface ThreadNamer {
        String platform();

        default String name(Class<?> context, String description) {
            StringBuilder sb = new StringBuilder();
            sb.append(platform());
            sb.append(" | ");
            sb.append(context.getSimpleName());
            sb.append(" | ");
            sb.append(description);
            return sb.toString();
        }
    }
}
