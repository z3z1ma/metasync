import itertools
from typing import List

from metabase.interface import MetabaseInstance
from metabase.model import PermissionGroup, PermissionMembership, User


async def sync_users(
    metabase: MetabaseInstance,
    *,
    yml_users: List[User],
    srv_users: List[User],
) -> None:
    # TODO: handle dry run
    diff_fields = {"email", "first_name", "last_name", "is_active"}
    keys = set(u.email for u in itertools.chain(yml_users, srv_users))
    for key in keys:
        srv = next((u for u in srv_users if u.email == key), None)
        yml = next((u for u in yml_users if u.email == key), None)
        srv.is_equal
        if srv and yml:
            if set(yml.dict(include=diff_fields).items()) - set(
                srv.dict(include=diff_fields).items()
            ):
                print(f"User '{key}' has been modified in the remote server")
                yml.id = srv.id
                if yml.is_active and not srv.is_active:
                    await metabase.user.reactivate(srv)
                elif not yml.is_active and srv.is_active:
                    await metabase.user.delete(srv)
                    continue
                await metabase.user.update(yml)
        elif srv and not yml:
            print(f"User '{key}' has been removed from the remote server")
            await metabase.user.delete(srv)
        elif not srv and yml:
            print(f"User '{key}' has been added to the remote server")
            await metabase.user.create(yml)


async def sync_groups(
    metabase: MetabaseInstance,
    *,
    yml_groups: List[PermissionGroup],
    srv_groups: List[PermissionGroup],
) -> None:
    # TODO: handle dry run
    diff_fields = {"name"}
    keys = set(u.name for u in itertools.chain(yml_groups, srv_groups))
    for key in keys - {"All Users", "Administrators"}:
        srv = next((u for u in srv_groups if u.name == key), None)
        yml = next((u for u in yml_groups if u.name == key), None)
        if srv and yml:
            if set(yml.dict(include=diff_fields).items()) - set(
                srv.dict(include=diff_fields).items()
            ):
                print(f"PermissionGroup '{key}' has been modified in the remote server")
                yml.id = srv.id
                # await metabase.permission_group.update(yml)
        elif srv and not yml:
            print(f"PermissionGroup '{key}' has been removed from the remote server")
            # await metabase.permission_group.delete(srv)
        elif not srv and yml:
            print(f"PermissionGroup '{key}' has been added to the remote server")
            # await metabase.permission_group.create(yml)
