# copyright 2022 Medicines Discovery Catapult
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest


class TempThrowable(Exception):
    pass


class TestKleinQueueError:

    def test_raise_exception(self):
        
        from src.klein_queue.errors import KleinQueueError
        with pytest.raises(KleinQueueError) as exc_info:
            try: 
                try: 
                    raise TempThrowable("bad mojo")
                except TempThrowable as err:
                    raise KleinQueueError(str(err), requeue=True) from err
            except KleinQueueError as ker:
                assert(isinstance(ker.__cause__, TempThrowable))
                assert str(ker.__cause__) == "bad mojo"
                assert ker.requeue
                raise ker
