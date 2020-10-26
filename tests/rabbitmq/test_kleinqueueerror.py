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
                    raise KleinQueueError from err
            except KleinQueueError as ker:
                assert(isinstance(ker.__cause__, TempThrowable))
                assert(str(ker.__cause__) == "bad mojo")
                raise ker
