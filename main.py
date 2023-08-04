# -*- coding:utf-8 -*-
import argparse
import sys
import os
import execute as e
import log

logger = log.Log()

class InputParser(object):

    def __init__(self):
        self.parser = argparse.ArgumentParser(description="Basic Setting for operating system")
        self.setup_parser()
        self.conf_args = {}

    def setup_parser(self):
        subp = self.parser.add_subparsers(metavar='', dest='subargs_basic')
        self.parser.add_argument('-v',
                                 '--version',
                                 dest='version',
                                 help='Show current version',
                                 action='store_true')

        self.parser_create = subp.add_parser("create", aliases=['c'], help="Create snapshot")

        self.parser_create.add_argument('-r',
                                       '--resource',
                                       dest='resource',
                                       help='Resource name',
                                       action='store')

        self.parser_dump = subp.add_parser("dump", aliases=['d'], help="Dump image")

        self.parser_dump.add_argument('-r',
                                       '--resource_name',
                                       dest='snapshot_resource',
                                       help='Resource name',
                                       action='store')
        self.parser_dump.add_argument('-s',
                                       '--snapshot_name',
                                       dest='snapshot_snapshot',
                                       help='Snapshot name',
                                       action='store')
        self.parser_dump.add_argument('-p',
                                       '--path',
                                       dest='image_path',
                                       help='Image path',
                                       action='store')

        self.parser_restore = subp.add_parser("snapshotrestore", aliases=['s'], help="Restore resource via snapshot")
        
        self.parser_restore.add_argument('-r',
                                       '--restore_resource',
                                       dest='restore_resource_s',
                                       help='Resource name',
                                       action='store')
        self.parser_restore.add_argument('-s',
                                       '--restore_snapshot',
                                       dest='restore_snapshot',
                                       help='Snapshot name',
                                       action='store')

        self.parser_original = subp.add_parser("imagerestore", aliases=['i'], help="Restore resource via image")
        
        self.parser_original.add_argument('-r',
                                       '--restore_resource',
                                       dest='restore_resource_i',
                                       help='Resource name',
                                       action='store')
        self.parser_original.add_argument('-i',
                                       '--restore_image',
                                       dest='restore_image',
                                       help='Restore image',
                                       action='store')
        self.parser_original.add_argument('-sp',
                                       '--storage_pool',
                                       dest='sp',
                                       help='Storage pool',
                                       action='store')

        self.parser_list = subp.add_parser("list", aliases=['l'], help="list")

        self.parser_list.add_argument('-D',
                                       '--DRBD',
                                       dest='list_drbd',
                                       help='List resource',
                                       action='store')
        self.parser_list.add_argument('-s',
                                       '--snapshot',
                                       dest='list_snapshot',
                                       help='List snapshot',
                                       action='store')

        self.parser_rollback = subp.add_parser("rollback", aliases=['rb'], help="rollback")

        self.parser_rollback.add_argument('-r',
                                       '--resource',
                                       dest='rollback_resource',
                                       help='Rollback resource',
                                       action='store')

        self.parser_create.set_defaults(func=self.create_func)
        self.parser_dump.set_defaults(func=self.dump_func)
        self.parser_restore.set_defaults(func=self.restore_func)
        self.parser_original.set_defaults(func=self.original_func)
        self.parser_list.set_defaults(func=self.list_func)
        self.parser_rollback.set_defaults(func=self.rollback_func)
        self.parser.set_defaults(func=self.help_usage)

    def create_func(self, args):
        logger.write_to_log("INFO", f"开始为资源'{args.resource}'创建快照", True)
        e.create_snap(args.resource)
      
    def dump_func(self, args):
        if args.image_path:
            logger.write_to_log("INFO", f"开始将资源'{args.snapshot_resource}'的快照'{args.snapshot_snapshot}'导出到目录'{args.image_path}'", True)
            e.dump_snap(args.snapshot_resource, args.snapshot_snapshot, args.image_path)
        else:
            logger.write_to_log("INFO", f"开始将资源'{args.snapshot_resource}'的快照'{args.snapshot_snapshot}'导出到默认目录", True)
            e.dump_snap(args.snapshot_resource, args.snapshot_snapshot)

    def restore_func(self, args):
        if args.restore_resource_s:
            logger.write_to_log("INFO", f"开始使用快照'{args.restore_snapshot}'对资源'{args.restore_resource_s}'进行恢复", True)
            e.snapshot_judge(args.restore_resource_s, args.restore_snapshot)
      
    def original_func(self, args):
        if args.restore_resource_i:
            logger.write_to_log("INFO", f"开始使用映像文件'{args.restore_image}'在卷组'{args.sp}'对资源'{args.restore_resource_i}'进行恢复", True)
            e.image_judge(args.restore_resource_i, args.restore_image, args.sp)
      
    def list_func(self, args):
        if args.list_drbd:
            logger.write_to_log("INFO", f"展示资源'{args.list_drbd}'的所有备份相关信息", True)
        elif args.list_snapshot:
            logger.write_to_log("INFO", f"展示快照'{args.list_snapshot}'的备份信息", True)
        else:
            logger.write_to_log("INFO", f"展示所有资源的快照信息", True)
        e.show_snap(args.list_drbd, args.list_snapshot)

    def rollback_func(self, args):
        logger.write_to_log("INFO", f"开始对资源'{args.rollback_resource}'进行回滚操作", True)
        e.rollback_judge(args.rollback_resource)
      
    def help_usage(self, args):
        if args.version:
            print(f'Version: {consts.VERSION}')
        else:
            self.parser.print_help()

    def parse(self):  # 调用入口
        args = self.parser.parse_args()
        args.func(args)


def main():
    try:
        run_program = InputParser()
        run_program.parse()
    except KeyboardInterrupt:
        sys.stderr.write("\nClient exiting (received SIGINT)\n")


if __name__ == '__main__':
    main()
