""" CLI """
import argparse
import logging
from aws_glacier_manager import interface

_PARSER_ALL = 'PARSER_ALL'


def setup_parser():
    parser = argparse.ArgumentParser(description='Manage AWS Glacier storage.')
    subparsers = parser.add_subparsers(help='choose sub-command', required=True, dest='subcommand')
    parser_cfg = subparsers.add_parser('cfg', help='manage local configuration')
    parser_cfg.add_argument('--db_string', type=str, help='set database connection string')
    parser_cfg.add_argument('--default_vault', type=str, help='set default vault for new projects')
    parser_cfg.add_argument('--list_projects', action='store_true', help='show available projects')
    parser_project = subparsers.add_parser('project', help='sync actions')
    parser_project.add_argument('project', help='name of the project')
    proj_subparsers = parser_project.add_subparsers(help='choose project action', required=True, dest='action')
    proj_subparsers.add_parser('status', help='show sync status')
    parser_proj_set_local_root = proj_subparsers.add_parser('set_local_root',
                                                            help='reset local root for project, or create project')
    parser_proj_set_local_root.add_argument('local_root', type=str, help='new local root path')
    for cmd, help_cmd, help_arg in [
        ('add', 'add file or subfolder to sync', 'filepath'),
        ('remove', 'remove file or subfolder from sync', 'filepath')
    ]:
        proj_subparsers.add_parser(cmd, help=help_cmd).add_argument(
            'filepaths', nargs='+', help=help_arg
        )
    for cmd, help_cmd, help_arg in [
        ('update', 'check for changed content', 'filepath'),
        ('push', 'upload data', 'filepath'),
        ('pull', 'download data', 'filepath'),
    ]:
        proj_subparsers.add_parser(cmd, help=help_cmd).add_argument(
            'filepaths', nargs='*', default=_PARSER_ALL, help=help_arg
        )
    return parser


def process_cfg(db_string, list_projects, default_vault):
    print(db_string, list_projects, default_vault)
    config_interface = interface.LocalConfigInterface()
    if db_string:
        config_interface.db_string = db_string
    if default_vault:
        config_interface.default_vault = default_vault
    if list_projects:
        projects_local, projects_remote = config_interface.get_project_status()
        projects_local = list(projects_local.keys())
        vaults = [str(projects_remote.get(p)) for p in projects_local]
        remote_only = {p: v for p, v in projects_remote.items() if p not in projects_local}
        if len(projects_local):
            print('local projects:')
        for x in zip(projects_local, vaults):
            print('%s (%s)' % x)
        if len(remote_only):
            print('remote only projects:')
        for x in remote_only.items():
            print('%s (%s)' % x)
    print('current DB string:', config_interface.db_string)


def process_project(project, action, args):
    project = interface.ProjectInterface(project)
    if action == 'set_local_root':
        project.local_root = args.local_root
    elif action == 'status':
        # ToDo: print root, files, excluded, and sync status
        pass
    elif action == 'add':
        pass
    elif action == 'remove':
        pass
    elif action == 'update':
        pass
    elif action == 'push':
        pass
    elif action == 'pull':
        pass


def main():
    logging.basicConfig(level=logging.INFO)
    args = setup_parser().parse_args()
    if args.subcommand == 'cfg':
        process_cfg(args.db_string, args.list_projects, args.default_vault)
    elif args.subcommand == 'project':
        print(args)
        process_project(args.project, args.action, args)


if __name__ == '__main__':
    main()
