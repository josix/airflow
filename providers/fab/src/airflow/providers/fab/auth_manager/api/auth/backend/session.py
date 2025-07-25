# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Session authentication backend."""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar, cast

from flask import Response

from airflow.api_fastapi.app import get_auth_manager

CLIENT_AUTH: tuple[str, str] | Any | None = None


def init_app(_):
    """Initialize authentication backend."""


T = TypeVar("T", bound=Callable)


def requires_authentication(function: T):
    """Decorate functions that require authentication."""

    @wraps(function)
    def decorated(*args, **kwargs):
        if not get_auth_manager().is_logged_in():
            return Response("Unauthorized", 401, {})
        return function(*args, **kwargs)

    return cast("T", decorated)
