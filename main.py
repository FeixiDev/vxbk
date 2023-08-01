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
        self.parser_dump.add_argument('-p',
                                       '--path',
                                       dest='image_path',
                                       help='Image path',
                                       action='store')

        self.parser_restore = subp.add_parser("restore", aliases=['r'], help="Restore resource via snapshot")
        
        self.parser_restore.add_argument('-f',
                                       '--file_resource',
                                       dest='file_resource',
                                       help='Resource name',
                                       action='store')
        self.parser_restore.add_argument('-b',
                                       '--block_resource',
                                       dest='block_resource',
                                       help='Resource name',
                                       action='store')
        self.parser_restore.add_argument('-s',
                                       '--restore_snapshot',
                                       dest='restore_snapshot',
                                       help='Snapshot name',
                                       action='store')

        self.parser_original = subp.add_parser("original", aliases=['o'], help="Restore resource via image")
        
        self.parser_original.add_argument('-f',
                                       '--file_resource',
                                       dest='file_original',
                                       help='Resource name',
                                       action='store')
        self.parser_original.add_argument('-b',
                                       '--block_resource',
                                       dest='block_original',
                                       help='Resource name',
                                       action='store')
        self.parser_original.add_argument('-i',
                                       '--original_image',
                                       dest='original_image',
                                       help='Original image',
                                       action='store')
        self.parser_original.add_argument('-vg',
                                       '--volume_group',
                                       dest='vg',
                                       help='Volume group',
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

        self.parser_create.set_defaults(func=self.create_func)
        self.parser_dump.set_defaults(func=self.dump_func)
        self.parser_restore.set_defaults(func=self.restore_func)
        self.parser_original.set_defaults(func=self.original_func)
        self.parser_list.set_defaults(func=self.list_func)
        self.parser.set_defaults(func=self.help_usage)

    def create_func(self, args):
        logger.write_to_log("INFO", f"开始为资源：'{args.resource}'创建快照", True)
        e.create_snap(args.resource)
      
    def dump_func(self, args):
        logger.write_to_log("INFO", f"开始将资源：'{args.snapshot_resource}'最新的快照导出到目录：'{args.image_path}'", True)
        e.dump_snap(args.snapshot_resource, args.image_path)

    def restore_func(self, args):
        if args.file_resource:
            logger.write_to_log("INFO", f"开始使用快照：'{args.restore_snapshot}'对资源：'{args.file_resource}'进行恢复", True)
            e.restore_file(args.file_resource, args.restore_snapshot)
        if args.block_resource:
            logger.write_to_log("INFO", f"开始使用快照：'{args.restore_snapshot}'对资源：'{args.block_resource}'进行恢复", True)
            e.restore_block(args.block_resource, args.restore_snapshot)
      
    def original_func(self, args):
        if args.file_original:
            logger.write_to_log("INFO", f"开始使用映像文件：'{args.original_image}'在卷组：'{args.vg}'对资源：'{args.file_original}'进行恢复", True)
            e.image_restore_file(args.file_original, args.original_image, args.vg)
        if args.block_original:
            logger.write_to_log("INFO", f"开始使用映像文件：'{args.original_image}'在卷组：'{args.vg}'对资源：'{args.block_original}'进行恢复", True)
            e.image_restore_block(args.block_original, args.original_image, args.vg)
      
    def list_func(self, args):
        if args.list_drbd:
            logger.write_to_log("INFO", f"展示资源：'{args.list_drbd}'的所有备份相关信息", True)
        elif args.list_snapshot:
            logger.write_to_log("INFO", f"展示快照：'{args.list_snapshot}'的备份信息", True)
        else:
            logger.write_to_log("INFO", f"展示所有资源的快照信息", True)
        e.show_snap(args.list_drbd, args.list_snapshot)
      
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
