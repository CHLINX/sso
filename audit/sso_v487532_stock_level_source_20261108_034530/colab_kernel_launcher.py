# Copyright 2023 Google Inc.
#
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
"""Custom kernel launcher app to customize socket options."""

from ipykernel import kernelapp
import zmq


# We want to set the high water mark on *all* sockets to 0, as we don't want
# the backend dropping any messages. We want to set this before any calls to
# bind or connect.
#
# In principle we should override `init_sockets`, but it's hard to set options
# on the `zmq.Context` there without rewriting the entire method. Instead we
# settle for only setting this on `iopub`, as that's the most important for our
# use case.
class ColabKernelApp(kernelapp.IPKernelApp):

  def init_iopub(self, context):
    context.setsockopt(zmq.RCVHWM, 0)
    context.setsockopt(zmq.SNDHWM, 0)
    return super().init_iopub(context)


if __name__ == '__main__':
  ColabKernelApp.launch_instance()
