from spaceone.core.error import *


class ERROR_JOB_STATE(ERROR_UNKNOWN):
    _message = 'Only running jobs can be canceled. (job_state = {job_state})'
