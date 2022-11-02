import asyncio
import os
import time
from pathlib import Path

from metabase import interface, model, serde, sync


async def test_1():
    metabase = interface.MetabaseInstance(
        host=os.environ["MB_HOST"],
        user=os.environ["MB_USER"],
        password=os.environ["MB_PASS"],
    )

    async with metabase.session:
        t1 = time.perf_counter()

        table = asyncio.create_task(metabase.table.list())
        table.add_done_callback(lambda _: print("Loaded tables 1"))

        database = asyncio.create_task(metabase.database.list())
        database.add_done_callback(lambda _: print("Loaded databases 2"))

        user = asyncio.create_task(metabase.user.list())
        user.add_done_callback(lambda _: print("Loaded users 3"))

        segment = asyncio.create_task(metabase.segment.list())
        segment.add_done_callback(lambda _: print("Loaded segments 4"))

        metric = asyncio.create_task(metabase.metric.list())
        metric.add_done_callback(lambda _: print("Loaded metrics 5"))

        card = asyncio.create_task(metabase.card.list())
        card.add_done_callback(lambda _: print("Loaded cards 6"))

        permission_group = asyncio.create_task(metabase.permission_group.list())
        permission_group.add_done_callback(
            lambda _: print("Loaded permission_groups 7")
        )

        _ = await asyncio.gather(
            table, database, user, segment, segment, metric, card, permission_group
        )

        t2 = time.perf_counter()
        print(t2 - t1)


async def test_2():
    metabase = interface.MetabaseInstance(
        host=os.environ["MB_HOST"],
        user=os.environ["MB_USER"],
        password=os.environ["MB_PASS"],
    )

    async with metabase.session:
        t1 = time.perf_counter()
        _ = await asyncio.gather(
            metabase.card.list(),
            metabase.table.list(),
            metabase.database.list(),
            metabase.user.list(),
            metabase.segment.list(),
            metabase.metric.list(),
            metabase.permission_group.list(),
        )
        t2 = time.perf_counter()
        print(t2 - t1)


async def test_3():
    metabase = interface.MetabaseInstance(
        host=os.environ["MB_HOST"],
        user=os.environ["MB_USER"],
        password=os.environ["MB_PASS"],
    )

    async with metabase.session:
        t1 = time.perf_counter()
        v1, v2, v3, v4, v5 = await asyncio.gather(
            metabase.dataset.native_query(database=2, query="select 1 as a"),
            metabase.dataset.native_query(database=2, query="select 2 as b"),
            metabase.dataset.native_query(database=2, query="select 3 as x"),
            metabase.dataset.native_query(database=2, query="select 4 as y"),
            metabase.dataset.native_query(database=2, query="select 5 as z"),
        )
        t2 = time.perf_counter()
        print(t2 - t1)
        print(v1)
        print(v2)
        print(v3)
        print(v4)
        print(v5)


async def test_4():
    metabase = interface.MetabaseInstance(
        host=os.environ["MB_HOST"],
        user=os.environ["MB_USER"],
        password=os.environ["MB_PASS"],
    )
    import time

    async with metabase.session:
        users, permissions = await asyncio.gather(
            metabase.user.list(), metabase.permission_membership.list()
        )

        async def _get_permissions(user_id):
            return [
                (await metabase.permission_group.get(p.group_id)).name
                for p in permissions
                if p.user_id == user_id
            ]

        async def _print(user: model.User):
            print(
                f"{user.common_name}: {user.last_login} -> {await _get_permissions(user.id)}"
            )

        t1 = time.perf_counter()

        tasks = []
        for user in users:
            tasks.append(asyncio.create_task(_print(user)))

        await asyncio.gather(*tasks)

        t2 = time.perf_counter()
        print(t2 - t1)


async def test_sandbox():
    metabase = interface.MetabaseInstance(
        host=os.environ["MB_HOST"],
        user=os.environ["MB_USER"],
        password=os.environ["MB_PASS"],
    )

    async with metabase.session:
        (
            server_users,
            server_databases,
            server_groups,
            server_pgraph,
        ) = await asyncio.gather(
            metabase.user.list(),
            metabase.database.list(),
            metabase.permission_group.list(),
            metabase.permission_graph.list(),
        )
        print("Syncing PermissionGroups")
        yaml_groups = serde.deserialize_groups(path=Path("output/output/groups.yml"))
        await sync.sync_groups(
            metabase, yml_groups=yaml_groups, srv_groups=server_groups
        )
        print("Synced")

        print("Syncing Users")
        yaml_users = serde.deserialize_users(path=Path("output/users.yml"))
        await sync.sync_users(
            metabase, yml_users=yaml_users, srv_users=server_users
        )
        print("Synced")

        serde.serialize_pgraph(
            Path("output/graph.yml"),
            pgraph=server_pgraph,
            groups=server_groups,
            databases=server_databases,
        )


if __name__ == "__main__":
    asyncio.run(test_sandbox())
