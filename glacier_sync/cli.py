from argparse import ArgumentParser


def main_init(args):
    pass


def main_list(args):
    pass


def main_add(args):
    pass


def main_push(args):
    pass


def main_pull(args):
    pass


def main_push_inventory(args):
    pass


SUBCOMMANDS = [
        ("init", "Initialize current directory as root for sync.", main_init),
        ("list", "Show files in inventory and sync status.", main_list),
        ("add", "Add files to inventory.", main_add),
        ("push", "Push missing chunks to vault.", main_push),
        ("pull", "Pull missing chunks from vault.", main_pull),
        ("push_inventory", "Push inventory to S3.", main_push_inventory),
    ]


def get_parser():
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()
    commands = {}
    for name, help_str, fcn in SUBCOMMANDS:
        command = subparsers.add_parser(name, help=help_str)
        command.set_defaults(main_fcn=fcn)
        commands[name] = command
    return parser


if __name__ == "__main__":
    ARGS = get_parser().parse_args()
    ARGS.main_fcn(ARGS)
