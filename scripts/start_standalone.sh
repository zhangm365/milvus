#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_PATH="${MILVUS_STANDALONE_LOG:-/tmp/standalone.log}"
PID_PATH="${MILVUS_STANDALONE_PID:-/tmp/milvus-standalone.pid}"

if [[ ! -x "${ROOT_DIR}/bin/milvus" ]]; then
	echo "ERROR: ${ROOT_DIR}/bin/milvus does not exist or is not executable." >&2
	exit 1
fi

if [[ -f "${PID_PATH}" ]]; then
	MILVUS_PID="$(cat "${PID_PATH}")"
	if kill -0 "${MILVUS_PID}" 2>/dev/null; then
		echo "Milvus standalone is already running (PID ${MILVUS_PID})."
		exit 0
	fi
	rm -f "${PID_PATH}"
fi

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
	LIBJEMALLOC="${ROOT_DIR}/internal/core/output/lib/libjemalloc.so"
	if test -f "$LIBJEMALLOC"; then
		#echo "Found $LIBJEMALLOC"
		export LD_PRELOAD="$LIBJEMALLOC"
		export MALLOC_CONF=background_thread:true
	else
		echo "WARN: Cannot find $LIBJEMALLOC"
	fi
	export LD_LIBRARY_PATH="${ROOT_DIR}/internal/core/output/lib/:${LD_LIBRARY_PATH:-}"
fi

export DEPLOY_MODE="${DEPLOY_MODE:-STANDALONE}"
export MILVUSCONF="${MILVUSCONF:-${ROOT_DIR}/configs}"

# Internal components advertise a host IP instead of loopback. Keep their gRPC
# traffic out of desktop HTTP/SOCKS proxies inherited from the shell.
LOCAL_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
if [[ -z "${LOCAL_IP}" ]]; then
	LOCAL_IP="$(ifconfig 2>/dev/null | awk '/inet / && $2 != "127.0.0.1" {print $2; exit}')"
fi
LOCAL_NO_PROXY="localhost,127.0.0.1,::1"
if [[ -n "${LOCAL_IP}" ]]; then
	LOCAL_NO_PROXY="${LOCAL_NO_PROXY},${LOCAL_IP}"
fi
export NO_PROXY="${NO_PROXY:+${NO_PROXY},}${LOCAL_NO_PROXY}"
export no_proxy="${no_proxy:+${no_proxy},}${LOCAL_NO_PROXY}"

echo "Starting standalone..."
nohup "${ROOT_DIR}/bin/milvus" run standalone --run-with-subprocess >"${LOG_PATH}" 2>&1 &
MILVUS_PID=$!
echo "${MILVUS_PID}" >"${PID_PATH}"
echo "Milvus standalone started (PID ${MILVUS_PID}). Log: ${LOG_PATH}"
