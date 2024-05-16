# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

"""Utility and helper methods"""

import os
import pathlib
import re
import ssl
import sys
import textwrap
import urllib
import xml.etree.ElementTree as elem_tree
from functools import reduce
from operator import getitem
from typing import Any, Optional, ClassVar

import certifi
import fire
import pandas as pd
import psutil
from packaging.version import Version
from pydantic import ValidationError, AnyHttpUrl, TypeAdapter

import spark_rapids_pytools
from spark_rapids_pytools import get_version
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_tools.exceptions import CspPathAttributeError


def get_elem_from_dict(data, keys):
    try:
        return reduce(getitem, keys, data)
    except LookupError:
        print(f'ERROR: Could not find elements [{keys}]')
        return None


def get_elem_non_safe(data, keys):
    try:
        return reduce(getitem, keys, data)
    except LookupError:
        return None


def stringify_path(fpath) -> str:
    if isinstance(fpath, str):
        actual_val = fpath
    elif hasattr(fpath, '__fspath__'):
        actual_val = os.fspath(fpath)
    else:
        raise CspPathAttributeError('Not a valid path')
    expanded_path = os.path.expanduser(actual_val)
    # make sure we return absolute path
    return os.path.abspath(expanded_path)


def is_http_file(value: Any) -> bool:
    try:
        TypeAdapter(AnyHttpUrl).validate_python(value)
        return True
    except ValidationError:
        # ignore
        return False


def get_path_as_uri(fpath: str) -> str:
    if re.match(r'\w+://', fpath):
        # that's already a valid url
        return fpath
    # stringify the path to apply the common methods which is expanding the file.
    local_path = stringify_path(fpath)
    return pathlib.PurePath(local_path).as_uri()


def to_camel_case(word: str) -> str:
    return word.split('_')[0] + ''.join(x.capitalize() or '_' for x in word.split('_')[1:])


def to_camel_capital_case(word: str) -> str:
    return ''.join(x.capitalize() for x in word.split('_'))


def to_snake_case(word: str) -> str:
    return ''.join(['_' + i.lower() if i.isupper() else i for i in word]).lstrip('_')


def dump_tool_usage(tool_name: Optional[str], raise_sys_exit: Optional[bool] = True):
    imported_module = __import__('spark_rapids_tools.cmdli', globals(), locals(), ['ToolsCLI'])
    wrapper_clzz = getattr(imported_module, 'ToolsCLI')
    help_name = 'spark_rapids'
    usage_cmd = f'{tool_name} -- --help'
    try:
        fire.Fire(wrapper_clzz(), name=help_name, command=usage_cmd)
    except fire.core.FireExit:
        # ignore the sys.exit(0) thrown by the help usage.
        # ideally we want to exit with error
        pass
    if raise_sys_exit:
        sys.exit(1)


def gen_app_banner() -> str:
    """
    ASCII Art is generated by an online Test-to-ASCII Art generator tool https://patorjk.com/software/taag
    :return: a string representing the banner of the user tools including the version
    """

    c_ver = spark_rapids_pytools.__version__
    return rf"""

********************************************************************
*                                                                  *
*    _____                  __      ____              _     __     *
*   / ___/____  ____ ______/ /__   / __ \____ _____  (_)___/ /____ *
*   \__ \/ __ \/ __ `/ ___/ //_/  / /_/ / __ `/ __ \/ / __  / ___/ *
*  ___/ / /_/ / /_/ / /  / ,<    / _, _/ /_/ / /_/ / / /_/ (__  )  *
* /____/ .___/\__,_/_/  /_/|_|  /_/ |_|\__,_/ .___/_/\__,_/____/   *
*     /_/__  __                  ______    /_/     __              *
*       / / / /_______  _____   /_  __/___  ____  / /____          *
*      / / / / ___/ _ \/ ___/    / / / __ \/ __ \/ / ___/          *
*     / /_/ (__  )  __/ /       / / / /_/ / /_/ / (__  )           *
*     \____/____/\___/_/       /_/  \____/\____/_/____/            *
*                                                                  *
*                                      Version. {c_ver}            *
*                                                                  *
* NVIDIA Corporation                                               *
* spark-rapids-support@nvidia.com                                  *
********************************************************************

"""


def init_environment(short_name: str):
    """
    Initialize the Python Rapids tool environment.
    Note:
    - This function is not implemented as the `__init__()` method to avoid execution
      when `--help` argument is passed.
    """
    # Set the 'UUID' environment variable with a unique identifier.
    uuid = Utils.gen_uuid_with_ts(suffix_len=8)
    Utils.set_rapids_tools_env('UUID', uuid)

    # Set the 'tools_home_dir' to store logs and other configuration files.
    home_dir = Utils.get_sys_env_var('HOME', '/tmp')
    tools_home_dir = FSUtil.build_path(home_dir, '.spark_rapids_tools')
    Utils.set_rapids_tools_env('HOME', tools_home_dir)

    # Set the 'LOG_FILE' environment variable and create the log directory.
    log_dir = f'{tools_home_dir}/logs'
    log_file = f'{log_dir}/{short_name}_{uuid}.log'
    Utils.set_rapids_tools_env('LOG_FILE', log_file)
    FSUtil.make_dirs(log_dir)

    # Print the log file location
    print(Utils.gen_report_sec_header('Application Logs'))
    print(f'Location: {log_file}')
    print('In case of any errors, please share the log file with the Spark RAPIDS team.\n')


class Utilities:
    """Utility class used to enclose common helpers and utilities."""
    # Assume that any tools thread would need at least 8 GB of heap memory.
    min_jvm_heap_per_thread: ClassVar[int] = 8
    # Flag used to disable running tools in parallel. This is a temporary hack to reduce possibility
    # of OOME. Later we can re-enable it.
    conc_mode_enabled: ClassVar[bool] = False

    @classmethod
    def get_latest_mvn_jar_from_metadata(cls, url_base: str,
                                         loaded_version: str = None) -> str:
        """
        Given the defined version in the python tools build, we want to be able to get the highest
        version number of the jar available for download from the mvn repo.
        The returned version is guaranteed to be LEQ to the defined version. For example, it is not
        allowed to use jar version higher than the python tool itself.

        The implementation relies on parsing the "$MVN_REPO/maven-metadata.xml" which guarantees
        that any delays in updating the directory list won't block the python module
        from pulling the latest jar.

        :param url_base: the base url from which the jar file is downloaded. It can be mvn repo.
        :param loaded_version: the version from the python tools in string format
        :return: the string value of the jar that should be downloaded.
        """

        if loaded_version is None:
            loaded_version = cls.get_base_release()
        context = ssl.create_default_context(cafile=certifi.where())
        defined_version = Version(loaded_version)
        jar_version = Version(loaded_version)
        xml_path = f'{url_base}/maven-metadata.xml'
        with urllib.request.urlopen(xml_path, context=context) as resp:
            xml_content = resp.read()
            xml_root = elem_tree.fromstring(xml_content)
            for version_elem in xml_root.iter('version'):
                curr_version = Version(version_elem.text)
                if curr_version <= defined_version:
                    jar_version = curr_version
        # get formatted string
        return cls.reformat_release_version(jar_version)

    @classmethod
    def reformat_release_version(cls, defined_version: Version) -> str:
        # get the release from version
        version_tuple = defined_version.release
        version_comp = list(version_tuple)
        # release format is under url YY.MM.MICRO where MM is 02, 04, 06, 08, 10, and 12
        res = f'{version_comp[0]}.{version_comp[1]:02}.{version_comp[2]}'
        return res

    @classmethod
    def get_base_release(cls) -> str:
        """
        For now the tools_jar is always with major.minor.0.
        this method makes sure that even if the package version is incremented, we will still
        get the correct url.
        :return: a string containing the release number 22.12.0, 23.02.0, amd 23.04.0..etc
        """
        defined_version = Version(get_version(main=None))
        # get the release from version
        return cls.reformat_release_version(defined_version)

    @classmethod
    def get_valid_df_columns(cls, input_cols, input_df: pd.DataFrame) -> list:
        """
        Returns a subset of input_cols that are present in the input_df
        """
        return [col for col in input_cols if col in input_df.columns]

    @classmethod
    def get_system_memory_in_gb(cls) -> int:
        """
        Get the total system memory in GB. Ideally we only grab 80% of teh total-memory
        """
        ps_memory = psutil.virtual_memory()
        return int(0.8 * ps_memory.total / (1024 ** 3))

    @classmethod
    def get_max_jvm_threads(cls) -> int:
        """
        Get the total cpu_count.
        """
        # Maximum number of threads that can be used in the tools JVM.
        # cpu_count returns the logical number of cores. So, we take a 50% to get better representation
        # of physical cores.
        return min(6, (psutil.cpu_count() + 1) // 2)

    @classmethod
    def adjust_tools_resources(cls,
                               jvm_heap: int,
                               jvm_processes: int,
                               jvm_threads: Optional[int] = None) -> dict:
        """
        Calculate the number of threads to be used in the Rapids Tools JVM cmd.
        In concurrent mode, the profiler needs to have more heap, and more threads.
        """
        # The number of threads is calculated based on the total system memory and the JVM heap size
        # Each thread should at least be running within 8 GB of heap memory
        concurrent_mode = cls.conc_mode_enabled and jvm_processes > 1
        heap_unit = max(cls.min_jvm_heap_per_thread, jvm_heap // 3 if concurrent_mode else jvm_heap)
        # calculate the maximum number of threads.
        upper_threads = cls.get_max_jvm_threads() // 3 if concurrent_mode else cls.get_max_jvm_threads()
        if jvm_threads is None:
            # make sure that the qual threads cannot exceed maximum allowed threads
            num_threads_unit = min(upper_threads, max(1, heap_unit // cls.min_jvm_heap_per_thread))
        else:
            num_threads_unit = max(1, jvm_threads // 3) if concurrent_mode else jvm_threads

        # The Profiling will be assigned the remaining memory resources
        prof_heap = max(jvm_heap - heap_unit, cls.min_jvm_heap_per_thread) if concurrent_mode else heap_unit
        if jvm_threads is None:
            prof_threads = max(1, prof_heap // cls.min_jvm_heap_per_thread) if concurrent_mode else num_threads_unit
            # make sure that the profiler threads cannot exceed maximum allowed threads
            prof_threads = min(upper_threads, prof_threads)
        else:
            prof_threads = max(1, jvm_threads - num_threads_unit) if concurrent_mode else jvm_threads

        return {
            'qualification': {
                'jvmMaxHeapSize': heap_unit,
                'rapidsThreads': num_threads_unit
            },
            'profiling': {
                'jvmMaxHeapSize': prof_heap,
                'rapidsThreads': prof_threads
            }
        }

    @classmethod
    def squeeze_df_header(cls, df_row: pd.DataFrame, header_width: int) -> pd.DataFrame:
        for column in df_row.columns:
            if len(column) > header_width:
                new_column_name = textwrap.fill(column, header_width, break_long_words=False)
                if new_column_name != column:
                    df_row.columns = df_row.columns.str.replace(column,
                                                                new_column_name, regex=False)
        return df_row

    @classmethod
    def bytes_to_human_readable(cls, num_bytes: int) -> str:
        """
        Convert bytes to human-readable format up to PB
        """
        size_units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        i = 0
        while num_bytes >= 1024 and i < len(size_units) - 1:
            num_bytes /= 1024.0
            i += 1
        return f'{num_bytes:.2f} {size_units[i]}'
