""" CLI """
import sys
import argparse
import logging
from aws_glacier_manager import interface


def setup_parser():
    parser = argparse.ArgumentParser(description='Manage AWS Glacier storage.')
    subparsers = parser.add_subparsers(help='choose sub-command', required=True, dest='subcommand')
    # quick status check
    subparsers.add_parser('status', help='status overview')
    # init project
    parser_init = subparsers.add_parser('init', help="init project config in current folder")
    parser_init.add_argument('--modify', action='store_true',
                             help='modify exsting configuration if it does exist')
    parser_init.add_argument('--db_string', type=str, help='set database connection string')
    parser_init.add_argument('--vault', type=str, help='set vault for project (no files are moved!))')
    parser_init.add_argument(
        'project_name',
        type=str,
        help='project name'
    )
    # remote actions
    parser_remote = subparsers.add_parser(
        'remote', help='actions concerning remote data'
    )
    parser_remote.add_argument('--db_string', type=str, help='database connection string')
    remote_subparsers = parser_remote.add_subparsers(
        help='choose remote action',
        required=True,
        dest='action'
    )
    remote_subparsers.add_parser('list', help='list projects')
    remote_subparser_clone = remote_subparsers.add_parser('clone', help='clone project')
    remote_subparser_clone.add_argument('project', help='project name to clone', required=True)
    # project data sync
    parser_project = subparsers.add_parser('project', help='sync actions')
    proj_subparsers = parser_project.add_subparsers(help='choose project action', required=True, dest='action')
    # proj_subparsers.add_parser('status', help='show sync status')
    for cmd, help_cmd, help_arg in [
        ('add', 'add file or subfolder to sync', 'filepath'),
        ('remove', 'remove file or subfolder from sync', 'filepath')
    ]:
        subsubparser = proj_subparsers.add_parser(cmd, help=help_cmd)
        subsubparser.add_argument(
            'filepaths', nargs='+', help=help_arg
        )
    for cmd, help_cmd, help_arg in [
        ('update', 'check for changed content', 'filepath'),
        ('push', 'upload data', 'filepath'),
        ('pull', 'download data', 'filepath'),
    ]:
        subsubparser = proj_subparsers.add_parser(cmd, help=help_cmd)
        subsubparser.add_argument(
            'filepaths', nargs='*', help=help_arg
        )
        subsubparser.add_argument('--missing', action="store_true",
                                  help="only process missing files")
    return parser


# def process_cfg(db_string, list_projects, default_vault):
#     print(db_string, list_projects, default_vault)
#     config_interface = interface.LocalConfigInterface()
#     if db_string:
#         config_interface.db_string = db_string
#     if default_vault:
#         config_interface.vault = default_vault
#     if list_projects:
#         projects_local, projects_remote = config_interface.get_project_status()
#         projects_local = list(projects_local.keys())
#         vaults = [str(projects_remote.get(p)) for p in projects_local]
#         remote_only = {p: v for p, v in projects_remote.items() if p not in projects_local}
#         if len(projects_local):
#             print('local projects:')
#         for x in zip(projects_local, vaults):
#             print('%s (%s)' % x)
#         if len(remote_only):
#             print('remote only projects:')
#         for x in remote_only.items():
#             print('%s (%s)' % x)
#     print('current DB string:', config_interface.db_string)


# def process_project(project, action, args):
#     logger = logging.getLogger(__name__)
#     project = interface.ProjectInterface(project)
#     if action == 'set_local_root':
#         project.local_root = args.local_root
#         return
#     elif not project.exists:
#         logger.warning('project %s does not exist' % project)
#         return
#     if action == 'status':
#         # ToDo: print root, files, excluded, and sync status
#         pass
#     elif action == 'add':
#         project.db_add_files(args.filepaths)
#     elif action == 'remove':
#         project.db_remove_files(args.filepaths)
#     elif action == 'update':
#         pass
#     elif action == 'push':
#         pass
#     elif action == 'pull':
#         pass


def fail(msg):
    sys.exit(msg)


def main():
    logging.basicConfig(level=logging.INFO)
    opts = setup_parser().parse_args()
    print(opts)
    cfg = interface.LocalConfigInterface()
    if opts.subcommand == 'status':
        process_status(cfg)
    elif opts.subcommand == 'init':
        process_init(cfg, opts)
    elif opts.subcommand == 'remote':
        process_remote(cfg, opts)
    elif opts.subcommand == 'project':
        # todo: implement
        print(opts)
        process_project(cfg, opts)
    else:
        raise ValueError('illegal subcommand: %s' % opts.subcommand)


def process_project(cfg: interface.LocalConfigInterface, opts) -> None:
    try:
        project = interface.ProjectInterface.from_cfg(cfg)
    except ValueError as exc:
        project = None
        if 'DB connector' in exc.args[0]:
            fail('--db_string must be provided')
        elif 'vault' in exc.args[0]:
            fail('--vault must be set for project')
        else:
            raise
    assert project, opts
    action = opts.action
    if action == 'add':
        project.db_add_files(opts.filepaths)
    elif action == 'remove':
        project.db_remove_files(opts.filepaths)
    elif action == 'update':
        # ToDo: needs local database of sizes, checksums, and timestamp for last update!
        pass
    elif action == 'push':
        pass
    elif action == 'pull':
        pass
    else:
        fail('unknown action: %s' % action)


def process_remote(cfg: interface.LocalConfigInterface, opts) -> None:
    db_string = opts.db_string or cfg.db_string
    current_project = cfg.project
    if not db_string:
        fail('no database connection given! Use "--db_string"!')
    if opts.action == "list":
        print(interface.LocalConfigInterface.get_remote_project_status(db_string))
        # ToDo: prettify
    elif opts.action == "clone":
        if opts.project:
            fail('current folder already has a project: %s' % current_project)
        if cfg.db_string != db_string:
            print('replacing existing connection string.')
        cfg.db_string = db_string
        cfg.project = opts.project
        print('current folder set up for sync of project %s' % opts.project)
    else:
        fail("unknown action with remote: %s" % opts.action)


def process_status(cfg: interface.LocalConfigInterface) -> None:
    # error if no project defined
    if not cfg.project:
        fail('current directory does not contain a project configuration.')
    # todo: print pretty summary
    print(cfg.local_cfg)


def process_init(cfg: interface.LocalConfigInterface, opts) -> None:
    if cfg.project and not opts.modify:
        fail('project is already set up. Use "--modify" flag to update configuration.')
    modified = False
    if opts.project_name is not None:
        if not cfg.project:
            fail('New projects need a project_name.')
        if not opts.project_name:
            fail('project name must not be empty')
        cfg.project = opts.project_name
        modified = True
    if opts.db_string is not None:
        cfg.db_string = opts.db_string
        modified = True
    if opts.vault is not None:
        cfg.vault = opts.vault
        modified = True
    if not modified:
        fail('no options given -> no init')


if __name__ == '__main__':
    main()
