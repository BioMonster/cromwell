#!/usr/bin/env bash

set -e
export CROMWELL_BUILD_SUPPORTS_CRON=true
# import in shellcheck / CI / IntelliJ compatible ways
# shellcheck source=/dev/null
source "${BASH_SOURCE%/*}/test.inc.sh" || source test.inc.sh

cromwell::build::setup_common_environment

cromwell::build::setup_centaur_environment

cromwell::build::setup_secure_resources

cromwell::build::assemble_jars

GOOGLE_AUTH_MODE="service-account"
GOOGLE_REFRESH_TOKEN_PATH="${CROMWELL_BUILD_RESOURCES_DIRECTORY}/papi_refresh_token.txt"
# This service account does not have billing permission, and therefore cannot be used for requester pays
GOOGLE_SERVICE_ACCOUNT_JSON="${CROMWELL_BUILD_RESOURCES_DIRECTORY}/cromwell-centaur-service-account.json"
# This service account does have billing permission and can be used for requester pays
GOOGLE_SERVICE_ACCOUNT_JSON_REQUESTER_PAYS="${CROMWELL_BUILD_RESOURCES_DIRECTORY}/cromwell-centaur-requester-pays-service-account.json"

# Export variables used in conf files
export GOOGLE_AUTH_MODE
export GOOGLE_REFRESH_TOKEN_PATH
export GOOGLE_SERVICE_ACCOUNT_JSON
export GOOGLE_SERVICE_ACCOUNT_JSON_REQUESTER_PAYS

# pass integration directory to the inputs json otherwise remove it from the inputs file
INTEGRATION_TESTS=()
if [ "${CROMWELL_BUILD_IS_CRON}" = "true" ]; then
    INTEGRATION_TESTS=(-i "${CROMWELL_BUILD_CENTAUR_INTEGRATION_TESTS}")
    # Increase concurrent job limit to get tests to finish under three hours.
    # Increase read_lines limit because of read_lines call on hg38.even.handcurated.20k.intervals.
    CENTAUR_READ_LINES_LIMIT=512000
    export CENTAUR_READ_LINES_LIMIT
fi

# Excluded tests:
# docker_hash_dockerhub_private: https://github.com/broadinstitute/cromwell/issues/3587

centaur/test_cromwell.sh \
    -j "${CROMWELL_BUILD_JAR}" \
    -c "${CROMWELL_BUILD_RESOURCES_DIRECTORY}/papi_v2_application.conf" \
    -p 100 \
    -g \
    -e localdockertest \
    "${INTEGRATION_TESTS[@]}"

cromwell::build::generate_code_coverage
