from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, TypeVar

from pydantic import BaseModel
from pydantic import Field as PydanticField
from pydantic import validator
from typing_extensions import Self


class MetabaseModel(BaseModel):
    """A base Metabase model"""

    id: int

    def is_equal(self, other: Self) -> bool:
        """
        Whether an MetabaseModel should be considered equal
        to a given MetabaseModel. Used to determine if a
        Resource should be updated.
        """
        raise NotImplementedError

    def can_delete(self) -> bool:
        """
        Whether a resource can be deleted if it is not found in the config.
        Some objects are protected and should never be deleted (i.e. Administrators group).
        """
        return True


class User(MetabaseModel):
    """A Metabase User"""

    id: int
    email: str
    first_name: str
    last_name: str
    common_name: str
    locale: Optional[str]

    is_superuser: Optional[bool]
    is_active: Optional[bool]
    is_qbnewb: Optional[bool]
    ldap_auth: Optional[bool]
    google_auth: Optional[bool]

    login_attributes: Optional[Dict[str, Any]] = PydanticField(default_factory=dict)
    group_ids: Optional[List[int]]

    last_login: Optional[datetime]
    date_joined: Optional[datetime]
    updated_at: Optional[datetime]

    @validator("common_name")
    def set_common_name(cls, v, values, **kwargs):
        return v or values["first_name"] + " " + values["last_name"]


class Database(MetabaseModel):
    """A Metabase Database"""

    id: int
    name: str
    description: Optional[str]
    engine: str

    features: List[Any]
    details: Dict[str, str]
    options: Optional[str]
    native_permissions: Optional[str]

    timezone: str
    metadata_sync_schedule: str
    cache_field_values_schedule: str
    cache_ttl: Optional[int]

    caveats: Optional[str]
    refingerprint: Optional[str]
    points_of_interest: Optional[str]

    auto_run_queries: bool
    is_full_sync: bool
    is_on_demand: bool
    is_sample: bool

    updated_at: str
    created_at: str


class Field(MetabaseModel):
    """A Metabase field"""

    id: int
    table_id: int
    fk_target_id: Optional[int]
    parent_id: Optional[int]

    name: str
    display_name: str
    description: Optional[str]

    database_type: str
    semantic_type: Optional["Field.SemanticType"]
    effective_type: str
    base_type: str

    dimensions: List[str] = PydanticField(default_factory=list)
    dimension_options: List[str] = PydanticField(default_factory=list)
    default_dimension_option: Optional[int]

    database_position: int
    custom_position: int
    visibility_type: "Field.VisibilityType"
    points_of_interest: Optional[str]
    has_field_values: Optional["Field.FieldValue"]

    active: bool
    preview_display: bool

    target: Optional["Field"]

    settings: Optional[dict] = PydanticField(default_factory=dict)
    caveats: Optional[str]
    coercion_strategy: Optional[str]

    updated_at: str
    created_at: str

    class VisibilityType(str, Enum):
        details_only = "details-only"
        hidden = "hidden"
        normal = "normal"
        retired = "retired"
        sensitive = "sensitive"

    class SemanticType(str, Enum):
        primary_key = "type/PK"
        foreign_key = "type/FK"
        author = "type/Author"
        avatar_url = "type/AvatarURL"
        biginteger = "type/BigInteger"
        birthdate = "type/Birthdate"
        boolean = "type/Boolean"
        cancelation_date = "type/CancelationDate"
        cancelation_time = "type/CancelationTime"
        cancelation_timestamp = "type/CancelationTimestamp"
        category = "type/Category"
        city = "type/City"
        comment = "type/Comment"
        company = "type/Company"
        cost = "type/Cost"
        country = "type/Country"
        creation_date = "type/CreationDate"
        creation_time = "type/CreationTime"
        creation_timestamp = "type/CreationTimestamp"
        currency = "type/Currency"
        datetime = "type/DateTime"
        datetime_local_tz = "type/DateTimeWithLocalTZ"
        decimal = "type/Decimal"
        deletion_date = "type/DeletionDate"
        deletion_time = "type/DeletionTime"
        deletion_timestamp = "type/DeletionTimestamp"
        description = "type/Description"
        discount = "type/Discount"
        duration = "type/Duration"
        email = "type/Email"
        enum = "type/Enum"
        gross_margin = "type/GrossMargin"
        image_url = "type/ImageURL"
        income = "type/Income"
        integer = "type/Integer"
        join_date = "type/JoinDate"
        join_time = "type/JoinTime"
        join_timestamp = "type/JoinTimestamp"
        latitude = "type/Latitude"
        longitude = "type/Longitude"
        name = "type/Name"
        number = "type/Number"
        owner = "type/Owner"
        price = "type/Price"
        product = "type/Product"
        quantity = "type/Quantity"
        score = "type/Score"
        serialized_json = "type/SerializedJSON"
        share = "type/Share"
        source = "type/Source"
        state = "type/State"
        subscription = "type/Subscription"
        text = "type/Text"
        title = "type/Title"
        updated_timestamp = "type/UpdatedTimestamp"
        url = "type/URL"
        user = "type/User"
        zip_code = "type/ZipCode"
        _ = "type/*"

    class FieldValue(str, Enum):
        none = "none"
        auto_list = "auto-list"
        list = "list"
        search = "search"


class DatabaseField(MetabaseModel):
    """A Metabase field as returned from database API"""

    id: int
    name: str
    base_type: Field.SemanticType
    semantic_type: Optional[Field.SemanticType]
    table_name: str
    table_schema: str = PydanticField(alias="schema")


FieldType = TypeVar("FieldType", Field, DatabaseField)
"""A bounded union type for generically representing a field"""


class Table(MetabaseModel):
    """A Metabase table"""

    id: int
    name: str
    table_schema: str = PydanticField(alias="schema")
    db_id: int
    db: Optional[dict] = PydanticField(default_factory=dict)

    display_name: str
    description: Optional[str]

    entity_type: Optional[str]
    entity_name: Optional[str]

    pk_field: Optional[int]
    visibility_type: Optional["Table.VisibilityType"]
    field_order: "Table.FieldOrder"
    points_of_interest: Optional[str]

    active: bool
    show_in_getting_started: bool

    created_at: str
    updated_at: str

    caveats: Optional[str]
    initial_sync_status: Optional[str]

    class VisibilityType(str, Enum):
        cruft = "cruft"
        hidden = "hidden"
        technical = "technical"

    class FieldOrder(str, Enum):
        alphabetical = "alphabetical"
        custom = "custom"
        database = "database"
        smart = "smart"


class Dimension(MetabaseModel):
    """A Metabase dimension"""

    id: str
    name: str
    mbql: Optional[List[Any]] = PydanticField(default_factory=list)
    type: str


class Metric(MetabaseModel):
    """A Metabase metric"""

    id: int
    database_id: int
    table_id: int
    creator_id: dict

    name: str
    description: str
    definition: dict
    how_is_this_calculated: str
    points_of_interest: str
    caveats: str

    archived: bool
    show_in_getting_started: bool

    created_at: datetime
    updated_at: datetime

    creator: dict


class Segment(MetabaseModel):
    """A Metabase segment"""

    id: int
    table_id: int
    creator_id: int

    name: str
    description: str
    definition: dict
    points_of_interest: str
    caveats: str

    show_in_getting_started: bool
    archived: bool

    updated_at: str
    created_at: str


class ColumnMetadata(BaseModel):
    """Metabase column metadata container"""

    display_name: str
    field_ref: List[Any]  # TODO: FieldRef type
    name: str
    base_type: Field.SemanticType
    effective_type: Field.SemanticType
    semantic_type: Optional[Field.SemanticType]
    fingerprint: Optional[dict]
    source: Optional[str]


class ResultMetadata(BaseModel):
    """Metabase result metadata container"""

    columns: List[ColumnMetadata]


class Data(BaseModel):
    """Metabase data container"""

    rows: List[List[Any]]
    cols: List[ColumnMetadata]
    native_form: dict
    results_metadata: ResultMetadata


class Dataset(BaseModel):
    """A Metabase dataset query"""

    context: str
    status: str
    database_id: int
    data: Data
    row_count: int

    started_at: datetime
    running_time: int
    json_query: dict
    average_execution_time: Optional[int]


class PermissionGroup(MetabaseModel):
    """A Metabase permission group"""

    id: int
    name: str
    member_count: Optional[int] = PydanticField(default=0)


class PermissionMembership(MetabaseModel):
    """A Metabase permission membership"""

    id: int = PydanticField(alias="membership_id")
    group_id: int
    user_id: int


class PermissionGraph(MetabaseModel):
    """A Metabase permission graph"""

    id: int = PydanticField(alias="revision")
    groups: Dict[int, Dict[int, Dict[str, Any]]]


class Card(MetabaseModel):
    """A Metabase Card"""

    id: int
    table_id: Optional[int]  # optional on native query?
    database_id: int
    collection_id: Optional[int]
    creator_id: int
    made_public_by_id: Optional[int]
    public_uuid: Optional[str]

    name: str
    description: Optional[str]

    collection: Optional[dict] = PydanticField(default_factory=dict)  # TODO: Collection
    collection_position: Optional[int]

    query_type: str
    dataset_query: dict  # TODO: DatasetQuery
    display: str
    visualization_settings: dict  # TODO: VisualizationSettings
    result_metadata: Optional[list]

    embedding_params: Optional[dict]
    cache_ttl: Optional[str]
    creator: User

    favorite: Optional[bool]
    archived: bool
    enable_embedding: bool

    updated_at: str
    created_at: str
