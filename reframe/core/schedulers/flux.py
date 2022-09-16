# Copyright 2016-2022 Swiss National Supercomputing Centre (CSCS/ETH Zurich)
# ReFrame Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: BSD-3-Clause

#
# Flux-Framework backend
#
# - Initial version submitted by Vanessa Sochat, Lawrence Livermore National Lab
#

import os
import time

import reframe.core.runtime as rt
from reframe.core.backends import register_scheduler
from reframe.core.exceptions import JobSchedulerError
from reframe.core.schedulers.pbs import PbsJobScheduler


# Just import flux once
try:
    import flux
    import flux.job
    from flux.job import JobspecV1
except ImportError:
    flux = None


@register_scheduler('flux')
class FluxJobScheduler(PbsJobScheduler):
    def __init__(self):
        if not flux:
            raise JobSchedulerError(
                "Cannot import flux. Is a cluster available to you with Python bindings?"
            )
        self._fexecutor = flux.job.FluxExecutor()
        self._submit_timeout = rt.runtime().get_option(
            f'schedulers/@{self.registered_name}/job_submit_timeout'
        )

    def _prepare_job_formatter(self):
        """
        A job doesn't have an easy status command - instead we do a job
        listing with a particular format. See src/cmd/flux-jobs.py#L44
        in flux-framework/flux-core for more attributes.
        """
        jobs_format = (
            "{id.f58:>12} {username:<8.8} {name:<10.10} {status_abbrev:>2.2} "
            "{ntasks:>6} {nnodes:>6h} {runtime!F:>8h} {success} {exception.occurred}"
            "{exception.note} {exception.type} {result} {runtime} {status}"
            "{ranks:h} {t_remaining} {annotations}"
        )
        self.jobs_formatter = flux.job.JobInfoFormat(jobs_format)

        # Note there is no attr for "id", its always returned
        fields2attrs = {
            "id.f58": (),
            "username": ("userid",),
            "exception.occurred": ("exception_occurred",),
            "exception.type": ("exception_type",),
            "exception.note": ("exception_note",),
            "runtime": ("t_run", "t_cleanup"),
            "status": ("state", "result"),
            "status_abbrev": ("state", "result"),
            "t_remaining": ("expiration", "state", "result"),
        }

        # Set job attributes we will use later to get job statuses
        self.job_attrs = set()
        for field in self.jobs_formatter.fields:
            if field not in fields2attrs:
                self.job_attrs.update((field,))
            else:
                self.job_attrs.update(fields2attrs[field])


    def emit_preamble(self, job):
        """
        We don't need to submit with a file, so we don't need a preamble.
        """
        return ["echo $PWD"]

    def submit(self, job):
        """
        Submit a job to the flux executor.
        """
        self._prepare_job_formatter()

        # Output and error files
        script_prefix = job.script_filename.split('.')[0]
        output = os.path.join(job.workdir, "%s.out" % script_prefix)
        error = os.path.join(job.workdir, "%s.err" % script_prefix)

        # Generate the flux job
        # Assume the filename includes a hashbang
        # flux does not support mem_mb, disk_mb
        fluxjob = JobspecV1.from_command(
            command=["/bin/bash", job.script_filename],
            num_tasks=job.num_tasks_per_core or 1,
            cores_per_task=job.num_cpus_per_task or 1
        )
        
        # A duration of zero (the default) means unlimited
        fluxjob.duration = job.time_limit or 0
        fluxjob.stdout = output
        fluxjob.stderr = error

        # This doesn't seem to be used?
        fluxjob.cwd = job.workdir
        fluxjob.environment = dict(os.environ)
        flux_future = self._fexecutor.submit(fluxjob)
        job._jobid = str(flux_future.jobid())
        job._submit_time = time.time()
        job._flux_future = flux_future


    def poll(self, *jobs):
        if jobs:
            # filter out non-jobs
            jobs = [job for job in jobs if job is not None]

        if not jobs:
            return

        # Loop through active jobs and act on status
        for job in jobs:
            if job._flux_future.done():

                # The exit code can help us determine if the job was successful
                try:
                    exit_code = job._flux_future.result(0)
                except RuntimeError:
                    # Assume some runtime issue (suspended)
                    self.log(f'Job {job.jobid} was likely suspended.')
                    job._state = 'SUSPENDED'    
                else:
                    # the job finished (but possibly with nonzero exit code)
                    if exit_code != 0:
                        self.log(f'Job {job.jobid} did not finish successfully')
                    job._state = 'COMPLETED'
                job._completed = True

            # Otherwise, we are still running
            else:
                job._state = 'RUNNING'

    def finished(self, job):
        if job.exception:
            raise job.exception

        return job.state == 'COMPLETED'
