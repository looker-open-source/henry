import threading
import sys
import time


class SpinnerThread(threading.Thread):

    def __init__(self):
        super().__init__(target=self._spin)
        self._stopevent = threading.Event()

    def stop(self):
        sys.stdout.write('\b')
        self._stopevent.set()

    def _spin(self):

        while not self._stopevent.isSet():
            for t in '|/-\\':
                sys.stdout.write(t)
                sys.stdout.flush()
                time.sleep(0.1)
                sys.stdout.write('\b')


class Spinner(object):
    def __enter__(self):
        self.spinner = SpinnerThread()
        self.spinner.start()

    def __exit__(self, exc_type, exc_value, tb):
        self.spinner.stop()
