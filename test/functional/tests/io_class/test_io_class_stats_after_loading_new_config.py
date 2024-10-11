#
# Copyright(c) 2024 Huawei Technologies Co., Ltd.
# SPDX-License-Identifier: BSD-3-Clause
#

import re
import pytest

from api.cas import casadm
from api.cas.cache_config import CacheMode, CleaningPolicy, SeqCutOffPolicy
from api.cas.ioclass_config import IoClass, default_config_file_path
from core.test_run import TestRun
from storage_devices.disk import DiskType, DiskTypeSet, DiskTypeLowerThan
from test_tools import fs_utils
from test_tools.disk_utils import Filesystem
from test_tools.fio.fio import Fio
from test_tools.fio.fio_param import IoEngine, ReadWrite
from test_utils.os_utils import sync
from test_utils.size import Size, Unit
from tests.io_class.io_class_common import template_config_path


@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
def test_io_class_stats_after_loading_new_config():
    """
    title: Open CAS statistics values per core for IO classification by file size.
    description: |
      Check Open CAS ability to assign correct IO class based on the file size.
      Test checking configuration with different filesystems on each core.
    pass_criteria:
        - after FIO with direct statistics increase only for direct IO class
    """

    new_io_class_count = 20

    with TestRun.step("Prepare devices."):
        cache_device = TestRun.disks["cache"]
        cache_device.create_partitions([Size(10, Unit.GibiByte)])
        cache_device = cache_device.partitions[0]

        core_device = TestRun.disks["core"]
        core_device.create_partitions([Size(15, Unit.GibiByte)])
        core_device = core_device.partitions[0]

    with TestRun.step("Start cache and add core device."):
        cache = casadm.start_cache(cache_device, CacheMode.WB, force=True)
        core = cache.add_core(core_device)

    with TestRun.step("Disable cleaning and sequential cutoff."):
        cache.set_cleaning_policy(CleaningPolicy.nop)
        cache.set_seq_cutoff_policy(SeqCutOffPolicy.never)

    with TestRun.step("Make filesystem on OpenCAS device and mount it."):
        mount_point = core.path.replace("/dev/", "/mnt/")
        core.create_filesystem(Filesystem.ext4)
        core.mount(mount_point)

    with TestRun.step("Load IO class configuration template file."):
        cache.load_io_class(template_config_path)

    with TestRun.step("Prepare and run fio matching IO classification file size based rules."):
        template_io_classes = IoClass.csv_to_list(fs_utils.read_file(template_config_path))
        file_size_based_io_classes = [
            io_class for io_class in template_io_classes if "file_size" in io_class.rule
        ]
        sizes = [
            *dict.fromkeys(
                [int(re.search(r"\d+", s.rule).group()) for s in file_size_based_io_classes]
            )
        ]
        sizes = [Size(size, Unit.Byte) for size in sizes]

        fio = (
            Fio()
            .create_command()
            .io_engine(IoEngine.libaio)
            .read_write(ReadWrite.randwrite)
            .io_depth(16)
            .block_size(Size(1, Unit.Blocks512))
        )

        for size in sizes:
            fio.add_job().file_size(size).io_size(size * 1.1).target(
                f"{core.mount_point}/file_{int(size.get_value())}"
            )

        fio.run()
        sync()

    with TestRun.step("Get Open CAS statistics and check if IO class statistics increased."):
        for class_id in range(file_size_based_io_classes[0].id, file_size_based_io_classes[-1].id):
            io_class_stats = cache.get_io_class_statistics(class_id)
            if io_class_stats.request_stats.requests_total == 0:
                TestRun.LOGGER.error(f"No request classified to IO class {class_id}.")
            if (
                io_class_stats.block_stats.cache.total == Size.zero()
                and io_class_stats.block_stats.exp_obj.total == Size.zero()
            ):
                TestRun.LOGGER.error(f"No block classified to IO class {class_id}.")

    with TestRun.step("Create and load new IO class config file."):
        random_io_classes = IoClass.generate_random_ioclass_list(new_io_class_count)
        IoClass.save_list_to_config_file(random_io_classes)
        cache.load_io_class(default_config_file_path)

    with TestRun.step("Check if Open CAS statistics are properly zeroed."):
        for io_class in random_io_classes:
            io_class_stats = cache.get_io_class_statistics(io_class.id)

            if io_class.rule == "unclassified":
                if (
                    io_class_stats.request_stats.requests_total == 0
                    or io_class_stats.block_stats.cache.total == Size.zero()
                    or io_class_stats.block_stats.exp_obj.total == Size.zero()
                ):
                    TestRun.LOGGER.error(
                        f"Stats for unclassified io class should be different than 0.\n "
                        f"{io_class_stats}"
                    )

            elif (
                io_class_stats.request_stats.requests_total != 0
                or io_class_stats.block_stats.cache.total != Size.zero()
                or io_class_stats.block_stats.exp_obj.total != Size.zero()
            ):
                TestRun.LOGGER.error(
                    f"Block and request stats for each IO class other than 'unclassified' "
                    f"should be zeroed.\n{io_class_stats}"
                )
