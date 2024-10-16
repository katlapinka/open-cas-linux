#
# Copyright(c) 2024 Huawei Technologies
# SPDX-License-Identifier: BSD-3-Clause
#
import pytest

from api.cas import casadm
from api.cas.cache_config import CacheLineSize
from api.cas.statistics import UnitType
from core.test_run import TestRun
from ocf.tests.functional.pyocf.types.cache import CleaningPolicy
from ocf.tests.functional.pyocf.types.shared import SeqCutOffPolicy
from storage_devices.disk import DiskTypeSet, DiskType, DiskTypeLowerThan
from test_tools.dd import Dd
from test_utils.os_utils import Udev
from test_utils.size import Size, Unit


@pytest.mark.parametrizex("cache_line_size", CacheLineSize)
@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
def test_seq_cutoff_multi_core(cache_mode, io_type, io_type_last, cache_line_size):
    """
    title: TBD
    description: |
        TBD
    pass_criteria:
      - TBD
    """

    with TestRun.step("Prepare cache and core devices"):
        cache_device = TestRun.disks["cache"]
        core_device = TestRun.disks["core"]

        cache_part = cache_device.partitions[0]
        core_parts = core_device.partitions

    with TestRun.step("Disable udev"):
        Udev.disable()

    with TestRun.step("Start cache and add core"):
        cache = casadm.start_cache(cache_part)
        core = cache.add_core(core_parts)

    with TestRun.step("Disable sequential cut-off and cleaning"):
        cache.set_seq_cutoff_policy(SeqCutOffPolicy.NEVER)
        cache.set_cleaning_policy(CleaningPolicy.NOP)

        dd = Dd().input("/dev/random") \
                 .output(core.path) \
                 .oflag("direct") \
                 .block_size(Size(1, Unit.Blocks4096)) \
                 .count(1)