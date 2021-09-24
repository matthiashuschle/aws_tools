from abc import ABC, abstractmethod
from argparse import ArgumentParser
from typing import Callable, Any, Dict
from . import interface


class SubCommand(ABC):

    command: str
    help_str: str
    _register: Dict[str, "SubCommand"] = {}

    @classmethod
    def create_all_for(cls, subparsers):
        for cmd in cls._register.values():
            cmd.add_command(subparsers)

    def __init_subclass__(cls, **kwargs):
        cls._register[cls.command] = cls

    @classmethod
    def add_command(cls, subparsers):
        command = subparsers.add_parser(cls.command, help=cls.help_str)
        command.set_defaults(main_fcn=cls.main_fcn)
        cls.add_arguments(command)

    @classmethod
    @abstractmethod
    def main_fcn(cls, args):
        pass

    @classmethod
    @abstractmethod
    def add_arguments(cls, command):
        pass


class SubCommandInit(SubCommand):

    command = "init"
    help_str = "Initialize current directory as root for sync."

    @classmethod
    def add_arguments(cls, command):
        pass

    @classmethod
    def main_fcn(cls, args):
        interface.init_local()


class SubCommandList(SubCommand):

    command = "list"
    help_str = "Show files in inventory and sync status."

    @classmethod
    def add_arguments(cls, command):
        pass

    @classmethod
    def main_fcn(cls, args):
        interface.list_files()


class SubCommandAdd(SubCommand):

    command = "add"
    help_str = "Add files to inventory."

    @classmethod
    def add_arguments(cls, command):
        pass

    @classmethod
    def main_fcn(cls, args):
        interface.add_files(files)


class SubCommandPush(SubCommand):

    command = "push"
    help_str = "Push missing chunks to vault."

    @classmethod
    def add_arguments(cls, command):
        pass

    @classmethod
    def main_fcn(cls, args):
        interface.push_any()


class SubCommandPull(SubCommand):

    command = "pull"
    help_str = "Pull missing chunks from vault."

    @classmethod
    def add_arguments(cls, command):
        pass

    @classmethod
    def main_fcn(cls, args):
        interface.pull_any()


class SubCommandPushInventory(SubCommand):

    command = "push_inventory"
    help_str = "Push inventory to S3."

    @classmethod
    def add_arguments(cls, command):
        pass

    @classmethod
    def main_fcn(cls, args):
        interface.push_inventory(s3_path)


def get_parser():
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()
    SubCommand.create_all_for(subparsers)
    return parser


if __name__ == "__main__":
    ARGS = get_parser().parse_args()
    ARGS.main_fcn(ARGS)
