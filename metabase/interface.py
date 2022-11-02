from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Set

import aiohttp
import requests
from pydantic import BaseModel

from metabase.exceptions import AuthenticationError, NotFoundError
from metabase.model import (
    Card,
    Database,
    DatabaseField,
    Dataset,
    Dimension,
    Field,
    MetabaseModel,
    Metric,
    PermissionGraph,
    PermissionGroup,
    PermissionMembership,
    Segment,
    Table,
    User,
)


class MetabaseInstance:
    """A metabase instance"""

    def __init__(self, host: str, user: str, password: str, token: str = None) -> None:
        self.host = host
        self.username = user
        self.password = password
        self._token = token
        # APIs
        self.card = CardAPI(self)
        self.user = UserAPI(self)
        self.database = DatabaseAPI(self)
        self.field = FieldAPI(self)
        self.table = TableAPI(self)
        self.metric = MetricAPI(self)
        self.segment = SegmentAPI(self)
        self.dataset = DatasetAPI(self)
        self.permission_group = PermissionGroupAPI(self)
        self.permission_membership = PermissionMembershipAPI(self)
        self.permission_graph = PermissionGraphAPI(self)

    @property
    def host(self) -> str:
        """The hostname of the metabase instance"""
        return self._host

    @host.setter
    def host(self, value: str) -> str:
        """Sets the host with some pre-defined cleansing for protocol and trailing /"""
        if not isinstance(value, str):
            raise TypeError("Metabase host must be set to string")
        if not value.startswith("http"):
            value = "https://" + value
        self._host = value.rstrip("/")

    @property
    def session(self) -> aiohttp.ClientSession:
        """Singleton session associated with the MetabaseInstance"""
        if not hasattr(self, "_session"):
            self._session = aiohttp.ClientSession(self.host)
        return self._session

    async def _get_token(self) -> str:
        """Performs authentication and returns a token"""
        response = await self.session.post(
            "/api/session",
            json={"username": self.username, "password": self.password},
        )
        if response.status != 200:
            raise AuthenticationError((await response.content.read()).decode())
        return (await response.json())["id"]

    @property
    async def token(self) -> str:
        """The Metabase session token used for all requests"""
        if self._token is None:
            self._token = await self._get_token()
        return self._token

    @token.setter
    def token(self, value) -> None:
        """Update the underlying token"""
        self._token = value

    @property
    async def headers(self) -> Dict[str, str]:
        """Supplies the X-Metabase-Session header with the instance token"""
        return {"X-Metabase-Session": await self.token}

    async def get(self, endpoint: str, **kwargs) -> aiohttp.ClientResponse:
        """Perform a GET request against an instance endpoint"""
        return await self.session.get(endpoint, headers=await self.headers, **kwargs)

    async def post(self, endpoint: str, **kwargs) -> aiohttp.ClientResponse:
        """Perform a POST request against an instance endpoint"""
        return await self.session.post(endpoint, headers=await self.headers, **kwargs)

    async def put(self, endpoint: str, **kwargs) -> aiohttp.ClientResponse:
        """Perform a PUT request against an instance endpoint"""
        return await self.session.put(endpoint, headers=await self.headers, **kwargs)

    async def delete(self, endpoint: str, **kwargs) -> aiohttp.ClientResponse:
        """Perform a DELETE request against an instance endpoint"""
        return await self.session.delete(endpoint, headers=await self.headers, **kwargs)


def _check_methods(C, *methods):
    """Method used to support issubclass interface for our traits"""
    mro = C.__mro__
    for method in methods:
        for B in mro:
            if method in B.__dict__:
                if B.__dict__[method] is None:
                    return NotImplemented
                break
        else:
            return NotImplemented
    return True


class Creatable(metaclass=ABCMeta):
    """Trait to be subclassed by a resource indicating it is creatable"""

    @abstractmethod
    async def create(self, entity: BaseModel, include: Set[str]) -> Dict[str, Any]:
        """Create a resource and save it."""
        assert isinstance(self, BaseMetabaseAPI)
        response: aiohttp.ClientResponse = await self._metabase.post(
            self.endpoint,
            json=entity.dict(
                exclude={"id"}, include=include, exclude_none=True, exclude_unset=True
            ),
        )
        if response.status not in (200, 202):
            raise requests.HTTPError((await response.content.read()).decode())
        return await response.json()

    async def create_from_kwargs(self, **kwargs) -> Dict[str, Any]:
        """Create a resource and save it. This permits kwargs and is thus pretty freeform."""
        assert isinstance(self, BaseMetabaseAPI)
        response: aiohttp.ClientResponse = await self._metabase.post(
            self.endpoint, json=kwargs
        )
        if response.status not in (200, 202):
            raise requests.HTTPError((await response.content.read()).decode())
        return await response.json()

    @classmethod
    def __subclasshook__(cls, C):
        """Allows using `issubclass(SomeResource, Creatable)`"""
        if cls is Creatable:
            return _check_methods(C, "create")
        return NotImplemented


class Gettable(metaclass=ABCMeta):
    """Trait to be subclassed by a resource indicating it is gettable"""

    @abstractmethod
    async def get(self, id: str, **kwargs) -> Dict[str, Any]:
        """Get a single resource by ID from the server."""
        assert isinstance(self, BaseMetabaseAPI)
        response = await self._metabase.get(
            f"{self.endpoint.rstrip('/')}/{id}", **kwargs
        )
        if response.status == 404 or response.status == 204:
            raise NotFoundError(f"{self}(id={id}) was not found.")
        return await response.json()

    @classmethod
    def __subclasshook__(cls, C):
        """Allows using `issubclass(SomeResource, Gettable)`"""
        if cls is Gettable:
            return _check_methods(C, "get")
        return NotImplemented


class Listable(metaclass=ABCMeta):
    """Trait to be subclassed by a resource indicating it is listable"""

    @abstractmethod
    async def list(self, **kwargs) -> List[Dict[str, Any]]:
        """List all resources on the server."""
        assert isinstance(self, BaseMetabaseAPI)
        response = await self._metabase.get(self.endpoint, **kwargs)
        return await response.json()

    @classmethod
    def __subclasshook__(cls, C):
        """Allows using `issubclass(SomeResource, Listable)`"""
        if cls is Listable:
            return _check_methods(C, "list")
        return NotImplemented


class Updateable(metaclass=ABCMeta):
    """Trait to be subclassed by a resource indicating it is updateable"""

    @abstractmethod
    async def update(
        self, entity: MetabaseModel, include: Set[str], **kwargs
    ) -> Dict[str, Any]:
        """Update resource on the server."""
        assert isinstance(self, BaseMetabaseAPI)
        response = await self._metabase.put(
            f"{self.endpoint.rstrip('/')}/{entity.id}",
            json=entity.dict(
                exclude={"id"}, include=include, exclude_none=True, exclude_unset=True
            ),
            **kwargs,
        )
        if response.status == 404 or response.status == 204:
            raise NotFoundError(f"{self}(id={entity.id}) was not found.")
        return await response.json()

    async def update_by_id(self, entity_id: int, **kwargs) -> Dict[str, Any]:
        """Update resource on the server by ID. Acceptance of kwargs makes this method flexible."""
        assert isinstance(self, BaseMetabaseAPI)
        response = await self._metabase.put(
            f"{self.endpoint.rstrip('/')}/{entity_id}", json=kwargs
        )
        if response.status == 404 or response.status == 204:
            raise NotFoundError(f"{self.__name__}(id={entity_id}) was not found.")
        return await response.json()

    @classmethod
    def __subclasshook__(cls, C):
        """Allows using `issubclass(SomeResource, Updateable)`"""
        if cls is Listable:
            return _check_methods(C, "update") and _check_methods(C, "update_by_id")
        return NotImplemented


class Deletable(metaclass=ABCMeta):
    """Trait to be subclassed by a resource indicating it is deletable"""

    @abstractmethod
    async def delete(self, entity: MetabaseModel) -> bool:
        """Delete a resource on the server."""
        return await self.delete_by_id(entity.id)

    async def delete_by_id(self, entity_id: int) -> bool:
        """Delete a resource on the server by ID."""
        assert isinstance(self, BaseMetabaseAPI)
        response = await self._metabase.delete(
            f"{self.endpoint.rstrip('/')}/{entity_id}"
        )
        if response.status not in (200, 204):
            raise requests.HTTPError((await response.content.read()).decode())
        return response.ok

    @classmethod
    def __subclasshook__(cls, C):
        """Allows using `issubclass(SomeResource, Deletable)`"""
        if cls is Listable:
            return _check_methods(C, "delete")
        return NotImplemented


class BaseMetabaseAPI:
    """Represents a base resource"""

    endpoint: str

    def __init__(self, metabase: MetabaseInstance):
        self._metabase = metabase

    def __repr__(self):
        return f"{self.__class__.__qualname__}(host={self._metabase.host}, endpoint={self.endpoint})"


class CardAPI(BaseMetabaseAPI, Creatable, Gettable, Listable, Updateable, Deletable):
    """Interface for Metabase cards API"""

    endpoint = "/api/card/"

    async def create(self, entity: Card):
        return await super().create(
            entity,
            include=[
                "name",
                "dataset_query",
                "visualization_settings",
                "display",
                "description",
                "collection_id",
                "collection_position",
                "result_metadata",
                "metadata_checksum",
                "cache_ttl",
            ],
        )

    async def get(self, id: int) -> Card:
        return Card(**await super().get(id))

    async def list(self) -> List[Card]:
        return [Card(**card) for card in await super().list()]

    async def update(self, entity: Card) -> Dict[str, Any]:
        return await super().update(
            entity,
            include={
                "name",
                "dataset_query",
                "visualization_settings",
                "display",
                "description",
                "collection_id",
                "collection_position",
                "result_metadata",
                "metadata_checksum",
                "archived",
                "enable_embedding",
                "embedding_params",
                "cache_ttl",
            },
        )

    async def update_by_id(self, entity_id: int, **kwargs) -> Dict[str, Any]:
        return await super().update_by_id(entity_id, **kwargs)

    async def delete(self, entity: Card) -> bool:
        return await super().delete(entity)

    async def archive(self, card_id: int):
        """Archive a Card."""
        return await self.update_by_id(
            card_id, {"archived": True, "revision_message": "Archived by MetaGit."}
        )


class UserAPI(BaseMetabaseAPI, Creatable, Gettable, Listable, Updateable, Deletable):
    """Interface for Metabase user API"""

    endpoint = "/api/user/"

    async def create(self, entity: User):
        return await super().create(
            entity,
            include={
                "first_name",
                "last_name",
                "email",
            },
        )

    async def get(self, id: int) -> User:
        return User(**await super().get(id))

    async def list(self) -> List[User]:
        return [
            User(**user)
            for user in (await super().list(params={"status": "all"}))["data"]
        ]

    async def update(self, entity: User) -> Dict[str, Any]:
        return await super().update(
            entity,
            include={
                "first_name",
                "last_name",
                "email",
                "is_active",
            },
        )

    async def delete(self, entity: User) -> bool:
        return await super().delete(entity)

    async def reactivate(self, entity: User) -> Dict[str, Any]:
        await self._metabase.put(f"{self.endpoint.rstrip('/')}/{entity.id}/reactivate")


class FieldAPI(BaseMetabaseAPI, Gettable, Updateable):
    """Interface for Metabase field API"""

    endpoint = "/api/field/"

    async def get(self, id: int) -> Field:
        return Field(**await super().get(id))

    async def update(self, entity: Field) -> Dict[str, Any]:
        return await super().update(
            entity,
            include={
                "display_name",
                "description",
                "semantic_type",
                "visibility_type",
                "fk_target_field_id",
                "has_field_values",
                "points_of_interest",
                "settings",
                "caveats",
                "coercion_strategy",
            },
        )

    async def update_by_id(self, entity_id: int, **kwargs) -> Dict[str, Any]:
        return await super().update_by_id(entity_id, **kwargs)

    async def related(self, field_id: int) -> Dict[str, Any]:
        """Return related entities."""
        related = await self._metabase.get(
            f"{self.endpoint.rstrip('/')}/{field_id}/related"
        )
        return await related.json()

    async def discard_values(self, field_id: int):
        """
        Discard the FieldValues belonging to this Field. Only applies to fields
        that have FieldValues. If this Field's Database is set up to automatically
        sync FieldValues, they will be recreated during the next cycle.

        You must be a superuser to do this.
        """
        return await self._metabase.post(
            f"{self.endpoint.rstrip('/')}/{field_id}/discard_values"
        )

    async def rescan_values(self, field_id: int):
        """
        Manually trigger an update for the FieldValues for this Field. Only applies
        to Fields that are eligible for FieldValues.

        You must be a superuser to do this.
        """
        return await self._metabase.post(
            f"{self.endpoint.rstrip('/')}/{field_id}/rescan_values"
        )


class DatabaseAPI(
    BaseMetabaseAPI, Creatable, Gettable, Listable, Updateable, Deletable
):
    """Interface for Metabase database API"""

    endpoint = "/api/database/"

    async def create(self, entity: Database):
        return await super().create(
            entity,
            include=[
                "name",
                "engine",
                "details",
                "is_full_sync",
                "is_on_demand",
                "schedules",
                "auto_run_queries",
                "cache_ttl",
            ],
        )

    async def get(self, id: int) -> Database:
        return Database(**await super().get(id))

    async def list(self) -> List[Database]:
        return [Database(**user) for user in (await super().list())["data"]]

    async def update(self, entity: Database) -> List[Dict[str, Any]]:
        return await super().update(
            entity,
            include={
                "name",
                "description",
                "engine",
                "schedules",
                "refingerprint",
                "points_of_interest",
                "auto_run_queries",
                "caveats",
                "is_full_sync",
                "cache_ttl",
                "details",
                "is_on_demand",
            },
        )

    async def update_by_id(self, entity_id: int, **kwargs) -> Dict[str, Any]:
        return await super().update_by_id(entity_id, **kwargs)

    async def delete(self, entity: Database) -> bool:
        return await super().delete(entity)

    async def fields(self, database_id: int) -> List[Field]:
        """Get a list of all Fields in Database."""
        fields = await self._metabase.get(
            f"{self.endpoint.rstrip('/')}/{database_id}/fields"
        )
        return [DatabaseField(**field) for field in await fields.json()]

    async def idfields(self, database_id: int) -> List[Field]:
        """Get a list of all primary key Fields for Database."""
        fields = await self._metabase.get(
            f"{self.endpoint.rstrip('/')}/{database_id}/idfields"
        )
        return [Field(**field) for field in await fields.json()]

    async def schemas(self, database_id: int) -> List[str]:
        """Returns a list of all the schemas found for the database id."""
        schemas = await self._metabase.get(
            f"{self.endpoint.rstrip('/')}/{database_id}/schemas"
        )
        return await schemas.json()

    async def tables(self, database_id: int, schema: str) -> List[Table]:
        """Returns a list of Tables for the given Database id and schema."""
        tables = await self._metabase.get(
            f"{self.endpoint.rstrip('/')}/{database_id}/schema/{schema}"
        )
        return [Table(**table) for table in await tables.json()]

    async def discard_values(self, database_id: int):
        """
        Discards all saved field values for this Database.

        You must be a superuser to do this.
        """
        return await self._metabase.post(
            f"{self.endpoint.rstrip('/')}/{database_id}/discard_values"
        )

    async def rescan_values(self, database_id: int):
        """
        Trigger a manual scan of the field values for this Database.

        You must be a superuser to do this.
        """
        return await self._metabase.post(
            f"{self.endpoint.rstrip('/')}/{database_id}/rescan_values"
        )

    async def sync(self, database_id: int):
        """Update the metadata for this Database. This happens asynchronously."""
        return await self._metabase.post(
            f"{self.endpoint.rstrip('/')}/{database_id}/sync"
        )

    async def sync_schema(self, database_id: int):
        """
        Trigger a manual update of the schema metadata for this Database.

        You must be a superuser to do this.
        """
        return await self._metabase.post(
            f"{self.endpoint.rstrip('/')}/{database_id}/sync_schema"
        )


class TableAPI(BaseMetabaseAPI, Gettable, Listable, Updateable):
    """Interface for Metabase table API"""

    endpoint = "/api/table/"

    async def get(self, id: int) -> Table:
        return Table(**await super().get(id))

    async def list(self) -> List[Table]:
        return [Table(**user) for user in await super().list()]

    async def update(self, entity: Table) -> List[Dict[str, Any]]:
        return await super().update(
            entity,
            include={
                "display_name",
                "description",
                "field_order",
                "visibility_type",
                "entity_type",
                "points_of_interest",
                "caveats",
                "show_in_getting_started",
            },
        )

    async def update_by_id(self, entity_id: int, **kwargs) -> Dict[str, Any]:
        return await super().update_by_id(entity_id, **kwargs)

    async def fks(self, table_id: int) -> List[Dict[str, Any]]:
        """Get all foreign keys whose destination is a Field that belongs to this Table."""
        fks = await self._metabase.get(f"{self.endpoint.rstrip('/')}/{table_id}/fks")
        return await fks.json()

    async def query_metadata(self, table_id: int) -> Dict[str, Any]:
        """
        Get metadata about a Table useful for running queries. Returns DB, fields,
        field FKs, and field values.

        Passing include_hidden_fields=true will include any hidden Fields in the response.
        Defaults to false Passing include_sensitive_fields=true will include any sensitive
        Fields in the response. Defaults to false.

        These options are provided for use in the Admin Edit Metadata page.
        """
        metadata = await self._metabase.get(
            f"{self.endpoint.rstrip('/')}/{table_id}/query_metadata"
        )
        return await metadata.json()

    async def related(self, table_id: int) -> Dict[str, Any]:
        """Return related entities."""
        related = await self._metabase.get(
            f"{self.endpoint.rstrip('/')}/{table_id}/related"
        )
        return await related.json()

    async def discard_values(self, table_id: int):
        """
        Discard the FieldValues belonging to the Fields in this Table. Only applies to
        fields that have FieldValues. If this Table’s Database is set up to automatically
        sync FieldValues, they will be recreated during the next cycle.

        You must be a superuser to do this.
        """
        return await self._metabase.post(
            f"{self.endpoint.rstrip('/')}/{table_id}/discard_values"
        )

    async def rescan_values(self, table_id: int):
        """
        Manually trigger an update for the FieldValues for the Fields belonging to this Table.
        Only applies to Fields that are eligible for FieldValues.

        You must be a superuser to do this.
        """
        return await self._metabase.post(
            f"{self.endpoint.rstrip('/')}/{table_id}/rescan_values"
        )

    async def fields(self, table_id: int) -> List[Field]:
        """Get all Fields associated with this Table.."""
        metadata = await self.query_metadata(table_id)
        return [Field(**field) for field in metadata.get("fields")]

    async def dimensions(self, table_id: int) -> List[Dimension]:
        """Get all Dimensions associated with this Table."""
        metadata = await self.query_metadata(table_id)
        return [
            Dimension(id=id, **dimension)
            for id, dimension in metadata.get("dimension_options", {}).items()
        ]

    async def metrics(self, table_id: int) -> List[Metric]:
        """Get all Metrics associated with this Table."""
        related = await self._metabase.get(
            f"{self.endpoint.rstrip('/')}/{table_id}/related"
        )
        return [Metric(**metric) for metric in related.get("metrics")]

    async def segments(self, table_id: int) -> List[Segment]:
        """Get all Segments associated with this Table."""
        related = await self._metabase.get(
            f"{self.endpoint.rstrip('/')}/{table_id}/related"
        )
        return [Segment(**segment) for segment in related.get("segments")]


class MetricAPI(BaseMetabaseAPI, Creatable, Gettable, Listable, Updateable):
    """Interface for Metabase metric API"""

    endpoint = "/api/metric/"

    async def get(self, id: int) -> Metric:
        return Metric(**await super().get(id))

    async def list(self) -> List[Metric]:
        return await super().list()

    async def update(self, entity: Metric) -> Dict[str, Any]:
        return await super().update(
            entity,
            include={
                "revision_message",
                "name",
                "description",
                "definition",
                "how_is_this_calculated",
                "points_of_interest",
                "caveats",
                "archived",
                "show_in_getting_started",
            },
        )

    async def update_by_id(self, entity_id: int, **kwargs) -> Dict[str, Any]:
        return await super().update_by_id(entity_id, **kwargs)

    async def create(self, entity: Metric):
        return await super().create(
            entity,
            include=[
                "name",
                "table_id",
                "definition",
                "description",
            ],
        )

    async def archive(self, metric_id: int):
        """Archive a Metric."""
        return await self.update_by_id(
            metric_id, {"archived": True, "revision_message": "Archived by MetaGit."}
        )


class SegmentAPI(BaseMetabaseAPI, Creatable, Gettable, Listable, Updateable):
    """Interface for Metabase segment API"""

    endpoint = "/api/segment/"

    async def get(self, id: int) -> Segment:
        return Segment(**await super().get(id))

    async def list(self) -> List[Segment]:
        return await super().list()

    async def update(self, entity: Segment) -> Dict[str, Any]:
        return await super().update(
            entity,
            include={
                "revision_message",
                "name",
                "description",
                "definition",
                "points_of_interest",
                "caveats",
                "archived",
                "show_in_getting_started",
            },
        )

    async def update_by_id(self, entity_id: int, **kwargs) -> Dict[str, Any]:
        return await super().update_by_id(entity_id, **kwargs)

    async def create(self, entity: Segment):
        return await super().create(
            entity,
            include=[
                "name",
                "table_id",
                "definition",
                "description",
            ],
        )

    async def archive(self, segment_id: int):
        """Archive a Segment."""
        return await self.update_by_id(
            segment_id, {"archived": True, "revision_message": "Archived by MetaGit."}
        )


class DatasetAPI(BaseMetabaseAPI, Creatable):
    """Interface for Metabase dataset API"""

    endpoint = "/api/dataset/"

    async def create(self, entity: Dataset) -> Dataset:
        # TODO: Create client side model for native / non-native queries
        raise NotImplemented

    async def native_query(
        self,
        database: int,
        query: str,
    ) -> Dataset:
        """Run a native SQL query against the database"""
        dataset = Dataset(
            **await super().create_from_kwargs(
                **{
                    "type": "native",
                    "native": {"query": query, "template-tags": {}},
                    "database": database,
                    "parameters": [],
                }
            )
        )
        return dataset


class PermissionGroupAPI(
    BaseMetabaseAPI, Creatable, Gettable, Listable, Updateable, Deletable
):
    """Interface for Metabase permission group API"""

    endpoint = "/api/permissions/group"

    async def get(self, id: int) -> PermissionGroup:
        """
        Fetch the details for a certain permissions group.

        You must be a superuser to do this.
        """
        return PermissionGroup(**await super().get(id))

    async def list(self) -> List[PermissionGroup]:
        """
        Fetch all PermissionsGroups, including a count of the number of :members in that group.

        You must be a superuser to do this.
        """
        return [PermissionGroup(**group) for group in await super().list()]

    async def update(self, entity: PermissionGroup) -> Dict[str, Any]:
        """
        Update the name of a PermissionsGroup.

        You must be a superuser to do this.
        """
        return await super().update(entity, include={"name"})

    async def update_by_id(self, entity_id: int, **kwargs) -> Dict[str, Any]:
        """
        Update the props of a PermissionsGroup by id.

        You must be a superuser to do this.
        """
        return await super().update_by_id(entity_id, **kwargs)

    async def create(self, entity: PermissionGroup):
        """
        Create a new PermissionsGroup.

        You must be a superuser to do this.
        """
        return await super().create(entity, include={"name"})

    async def delete(self, entity: MetabaseModel) -> bool:
        """
        Delete a specific PermissionsGroup.

        You must be a superuser to do this.
        """
        return await super().delete(entity)


class PermissionMembershipAPI(BaseMetabaseAPI, Creatable, Listable, Deletable):
    """Interface for Metabase permission membership API"""

    endpoint = "/api/permissions/membership"

    async def list(self) -> List[PermissionMembership]:
        """
        Fetch a map describing the group memberships of various users. This map's format is:

        {<user-id> [{:membership_id <id>
                     :group_id      <id>}]}.
        You must be a superuser to do this.
        """
        return [
            PermissionMembership(**permission)
            for user_id, permissions in (await super().list()).items()
            for permission in permissions
        ]

    async def create(self, entity: PermissionMembership):
        """
        Add a User to a PermissionsGroup. Returns updated list of members belonging to the group.

        You must be a superuser to do this.
        """
        return await super().create(entity, include={"group_id", "user_id"})

    async def delete(self, entity: MetabaseModel) -> bool:
        return await super().delete(entity)


class PermissionGraphAPI(BaseMetabaseAPI, Listable):
    """Interface for Metabase permission graph API"""

    endpoint = "/api/permissions/graph"

    async def list(self, **kwargs) -> PermissionGraph:
        return PermissionGraph(**await super().list(**kwargs))
