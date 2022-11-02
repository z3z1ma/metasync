from pathlib import Path
from typing import List

import ruamel.yaml

from metabase.model import (
    Database,
    PermissionGraph,
    PermissionGroup,
    PermissionMembership,
    User,
)

YAML = ruamel.yaml.YAML()


def serialize_users(
    path: Path,
    *,
    users: List[User],
) -> None:
    YAML.dump(
        [
            user.dict(
                exclude={
                    "id",
                    "common_name",
                    "date_joined",
                    "last_login",
                    "updated_at",
                    "is_superuser",
                    "is_qbnewb",
                    "ldap_auth",
                    "google_auth",
                    "locale",
                    "login_attributes",
                    "group_ids",
                }
            )
            for user in users
        ],
        path,
    )


def deserialize_users(path: Path) -> List[User]:
    return [
        User(
            **{
                "id": -1,
                "common_name": f'{user["first_name"]} {user["last_name"]}',
                **user,
            }
        )
        for user in YAML.load(path) or []
    ]


def serialize_groups(
    path: Path,
    *,
    groups: List[User],
) -> None:
    YAML.dump(
        [group.dict(exclude={"member_count", "id"}) for group in groups],
        path,
    )


def deserialize_groups(path: Path) -> List[PermissionGroup]:
    return [PermissionGroup(**{"id": -1, **group}) for group in YAML.load(path) or []]


def serialize_memberships(
    path: Path,
    *,
    memberships: List[PermissionMembership],
    users: List[User],
    groups: List[PermissionGroup],
) -> None:
    users_lookup = {
        k: v for k, v in zip([u.id for u in users], [u.email for u in users])
    }
    groups_lookup = {
        k: v for k, v in zip([g.id for g in groups], [g.name for g in groups])
    }
    agg_members_by_group = {g.name: [] for g in groups}
    for membership in memberships:
        agg_members_by_group[groups_lookup[membership.group_id]].append(
            users_lookup[membership.user_id]
        )
    YAML.dump(
        agg_members_by_group,
        path,
    )


def deserialize_memberships(
    path: Path,
    *,
    users: List[User],
    groups: List[PermissionGroup],
) -> List[PermissionMembership]:
    users_lookup = {
        k: v for k, v in zip([u.email for u in users], [u.id for u in users])
    }
    groups_lookup = {
        k: v for k, v in zip([g.name for g in groups], [g.id for g in groups])
    }
    return [
        PermissionMembership(
            **{
                "membership_id": -1,
                "user_id": users_lookup[member],
                "group_id": groups_lookup[group],
            }
        )
        for group, members in (YAML.load(path) or {}).items()
        for member in members
    ]


def serialize_pgraph(
    path: Path,
    *,
    pgraph: PermissionGraph,
    databases: List[Database],
    groups: List[PermissionGroup],
) -> None:
    database_lookup = {
        k: v for k, v in zip([u.id for u in databases], [u.name for u in databases])
    }
    groups_lookup = {
        k: v for k, v in zip([g.id for g in groups], [g.name for g in groups])
    }
    pgraph_obj = pgraph.dict(exclude={"revision"})
    groups = list(pgraph_obj["groups"].keys())
    graph = {}
    for group, database_permissions in pgraph.groups.items():
        perms = {}
        for database, permissions in database_permissions.items():
            perms[database_lookup[database]] = permissions
        graph[groups_lookup[group]] = perms
    YAML.dump(
        {"groups": graph},
        path,
    )
