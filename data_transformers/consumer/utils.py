import os
import sys

def find_file_downwards(filename, raise_error_if_not_found, usecwd, depth_limit):
    """
    Search in increasingly deeper folders for the given file

    Returns path to the file if found, or an empty string otherwise
    """
    def _is_interactive() -> bool:
        """ Decide whether this is running in a REPL or IPython notebook """
        try:
            main = __import__('__main__', None, None, fromlist=['__file__'])
        except ModuleNotFoundError:
            return False
        return not hasattr(main, '__file__')

    def _walk_to_depth(path: str, depth_limit: int):
        """ Generate directories from path downwards up to depth_limit """
        for root, dirs, files in os.walk(path):
            yield root
            current_depth = root[len(path):].count(os.sep)
            if depth_limit is not None and current_depth >= depth_limit:
                del dirs[:]  # Clear dirs to prevent walking further

    if usecwd or _is_interactive() or getattr(sys, 'frozen', False):
        # Should work without __file__, e.g. in REPL or IPython notebook.
        start_path = os.getcwd()
    else:
        # will work for .py files
        frame = sys._getframe()
        current_file = __file__

        while frame.f_code.co_filename == current_file or not os.path.exists(
            frame.f_code.co_filename
        ):
            assert frame.f_back is not None
            frame = frame.f_back
        frame_filename = frame.f_code.co_filename
        start_path = os.path.dirname(os.path.abspath(frame_filename))

    for dirname in _walk_to_depth(start_path, depth_limit):
        check_path = os.path.join(dirname, filename)
        if os.path.isfile(check_path):
            return check_path

    if raise_error_if_not_found:
        raise IOError(f"File '{filename}' not found")

    return ''

def find_file_upwards(filename, raise_error_if_not_found, usecwd, depth_limit):
        """
        Search in increasingly higher folders for the given file

        Returns path to the file if found, or an empty string otherwise
        """
        def _is_interactive() -> bool:
            """ Decide whether this is running in a REPL or IPython notebook """
            try:
                main = __import__('__main__', None, None, fromlist=['__file__'])
            except ModuleNotFoundError:
                return False
            return not hasattr(main, '__file__')

        def _walk_to_root(path: str, depth_limit: int):
            """ Generate directories from path to root, up to depth_limit """
            depth = 0
            while True:
                yield path
                new_path = os.path.dirname(path)
                if new_path == path or (depth_limit is not None and depth >= depth_limit):
                    break
                path = new_path
                depth += 1

        if usecwd or _is_interactive() or getattr(sys, 'frozen', False):
            # Should work without __file__, e.g. in REPL or IPython notebook.
            path = os.getcwd()
        else:
            # will work for .py files
            frame = sys._getframe()
            current_file = __file__

            while frame.f_code.co_filename == current_file or not os.path.exists(
                frame.f_code.co_filename
            ):
                assert frame.f_back is not None
                frame = frame.f_back
            frame_filename = frame.f_code.co_filename
            path = os.path.dirname(os.path.abspath(frame_filename))

        for dirname in _walk_to_root(path, depth_limit):
            check_path = os.path.join(dirname, filename)
            if os.path.isfile(check_path):
                return check_path

        if raise_error_if_not_found:
            raise IOError(f"File '{filename}' not found")

        return ''

def find_file(
    filename: str,
    raise_error_if_not_found: bool = False,
    usecwd: bool = False,
    upwards_depth_limit: int = None,
    downwards_depth_limit: int = None
) -> str:
    """
    Search for the given file in increasingly higher and lower folders

    Returns path to the file if found, or an empty string otherwise
    """

    result = find_file_upwards(filename, raise_error_if_not_found=False, usecwd=usecwd, depth_limit=upwards_depth_limit)
    if result:
        return result

    result = find_file_downwards(filename, raise_error_if_not_found=raise_error_if_not_found, usecwd=usecwd, depth_limit=downwards_depth_limit)
    return result

from typing import Any, ClassVar
class slicer:
    def __init__(self, stop, start=0, step=1):
        self._slice = slice(start, stop, step)

    @property
    def start(self) -> Any:
        return self._slice.start
    
    @property
    def step(self) -> Any:
        return self._slice.step
    
    @property
    def stop(self) -> Any:
        return self._slice.stop
    
    def __eq__(self, value: object, /) -> bool:
        return self._slice.__eq__(value)
    
    __hash__: ClassVar[None]  # type: ignore[assignment]

    def indices(self, *args) -> tuple[int, int, int]:
        return self._slice.indices(*args)
    
    def __call__(self, sliceable):
        return sliceable.__getitem__(self._slice)
    
    def __repr__(self):
        return repr(self._slice)