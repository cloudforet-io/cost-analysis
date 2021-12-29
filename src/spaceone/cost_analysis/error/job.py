from spaceone.core.error import *


class ERROR_JOB_STATE(ERROR_UNKNOWN):
    _message = 'Only running jobs can be canceled. (job_state = {job_state})'


class ERROR_DUPLICATE_JOB(ERROR_UNKNOWN):
    _message = 'The same job is already running. (data_source_id = {data_source_id})'

