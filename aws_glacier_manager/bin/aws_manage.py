""" CLI """
import argparse
from aws_glacier_manager import interface


def setup_parser():
    parser = argparse.ArgumentParser(description='Manage AWS Glacier storage.')
    subparsers = parser.add_subparsers(help='choose sub-command', required=True, dest='subcommand')
    parser_cfg = subparsers.add_parser('cfg', help='manage local configuration')
    parser_cfg.add_argument('--db_string', type=str, help='set database connection string')
    parser_cfg.add_argument('--default_vault', type=str, help='set default vault for new projects')
    parser_cfg.add_argument('--list_projects', action='store_true', help='show available projects')
    parser_project = subparsers.add_parser('project', help='sync actions')
    parser_project.add_argument('--status', action='store_true', help='show sync status')
    parser_project.add_argument('--set_local_root', type=str, help='map local path as project root folder')
    parser_project.add_argument('--create', action='store_true', help='create new project')
    parser_project.add_argument('project', help='name of the project')
    return parser


def process_cfg(db_string, list_projects, default_vault):
    print(db_string, list_projects, default_vault)
    config_interface = interface.ConfigInterface()
    if db_string:
        config_interface.db_string = db_string
    if default_vault:
        config_interface.default_vault = default_vault
    if list_projects:
        project_status = config_interface.get_project_status()
        print(project_status)
    print('current DB string:', config_interface.db_string)


def process_project(project, show_status, set_local_root, create):
    print(project, show_status, set_local_root, create)

    if set_local_root:
        # ToDo: set local root and write configuration
        pass
    if show_status:
        # ToDo: print root, files, excluded, and sync status
        pass


def main():
    args = setup_parser().parse_args()
    if args.subcommand == 'cfg':
        process_cfg(args.db_string, args.list_projects, args.default_vault)
    elif args.subcommand == 'project':
        process_project(args.project, args.status, args.set_local_root, args.create)


if __name__ == '__main__':
    main()
