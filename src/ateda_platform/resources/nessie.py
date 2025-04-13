from dagster import ConfigurableResource
from typing import Optional, Literal
from enum import Enum


class OAuth2GrantType(str, Enum):
    """Valid OAuth2 grant types for Nessie."""
    CLIENT_CREDENTIALS = "client_credentials"
    PASSWORD = "password"
    AUTHORIZATION_CODE = "authorization_code"
    DEVICE_CODE = "device_code"
    DEVICE_CODE_URN = "urn:ietf:params:oauth:grant-type:device_code"
    TOKEN_EXCHANGE = "token_exchange"
    TOKEN_EXCHANGE_URN = "urn:ietf:params:oauth:grant-type:token-exchange"


class NessieResource(ConfigurableResource):
    """Resource that provides access to Nessie catalog configuration.
    
    This resource centralizes all Nessie-related configuration and can be extended
    to include PyNessie client functionality for direct API interactions.
    
    Example:
        .. code-block:: python

            @asset(required_resource_keys={"nessie"})
            def my_asset(context):
                nessie_uri = context.resources.nessie.uri
                # Use the URI in Spark/Trino configuration
    """
    # Base config fields
    uri: str  # Required URI to Nessie service
    catalog_name: str = "nessie"  # Name of the catalog (used in Spark/Trino config)
    default_branch: str = "main"  # Default branch to use
    default_database: str = "default"  # Default database to use
    authentication_type: Literal["NONE", "BEARER", "OAUTH2", "AWS"] = "NONE"
    
    # Network settings
    request_timeout: Optional[int] = None  # Request timeout in seconds
    connect_timeout: Optional[int] = None  # Connection timeout in seconds
    max_retries: Optional[int] = None  # Maximum number of retries
    retry_delay: Optional[int] = None  # Delay between retries in milliseconds
    
    # HTTP settings
    max_connections: Optional[int] = None  # Maximum number of concurrent connections
    keep_alive: Optional[bool] = None  # Whether to keep connections alive
    compression_enabled: Optional[bool] = None  # Whether to enable compression
    
    # Bearer auth
    token: Optional[str] = None  # Token for BEARER auth
    
    # OAuth2 auth - Basic settings
    oauth2_issuer_url: Optional[str] = None  # OAuth2 issuer URL for endpoint discovery
    oauth2_token_endpoint: Optional[str] = None  # OAuth2 token endpoint
    oauth2_auth_endpoint: Optional[str] = None  # OAuth2 authorization endpoint
    oauth2_device_auth_endpoint: Optional[str] = None  # OAuth2 device authorization endpoint
    oauth2_client_id: Optional[str] = None  # OAuth2 client ID
    oauth2_client_secret: Optional[str] = None  # OAuth2 client secret
    oauth2_grant_type: Optional[str] = None
    
    # OAuth2 auth - Password grant settings
    oauth2_username: Optional[str] = None  # Username for password grant
    oauth2_password: Optional[str] = None  # Password for password grant
    
    # OAuth2 auth - Token exchange settings
    oauth2_token_exchange_subject_token: Optional[str] = None  # Subject token for token exchange
    oauth2_token_exchange_requested_token_type: Optional[str] = None  # Requested token type
    
    # OAuth2 auth - Impersonation settings
    oauth2_impersonation_enabled: Optional[bool] = None  # Whether to enable impersonation
    oauth2_impersonation_target: Optional[str] = None  # Target identity to impersonate
    oauth2_impersonation_token_endpoint: Optional[str] = None  # Token endpoint for impersonation
    
    # OAuth2 auth - Token refresh settings
    oauth2_refresh_enabled: Optional[bool] = None  # Whether to enable token refresh
    oauth2_refresh_before_expiry: Optional[int] = None  # Seconds before expiry to refresh token
    oauth2_refresh_token: Optional[str] = None  # Refresh token for token refresh
    oauth2_refresh_token_endpoint: Optional[str] = None  # Token endpoint for refresh
    
    # AWS auth
    aws_region: Optional[str] = None  # AWS region for AWS authentication

    def _validate_oauth2_config(self) -> None:
        """Validate OAuth2 configuration."""
        if self.authentication_type != "OAUTH2":
            return

        # Validate grant type
        if self.oauth2_grant_type not in [e.value for e in OAuth2GrantType]:
            raise ValueError(f"Invalid OAuth2 grant type: {self.oauth2_grant_type}")

        # Validate required properties for each grant type
        if self.oauth2_grant_type in [OAuth2GrantType.CLIENT_CREDENTIALS.value, 
                                    OAuth2GrantType.PASSWORD.value,
                                    OAuth2GrantType.AUTHORIZATION_CODE.value]:
            if not (self.oauth2_issuer_url or self.oauth2_token_endpoint):
                raise ValueError("Either oauth2_issuer_url or oauth2_token_endpoint must be provided")
            if not self.oauth2_client_id:
                raise ValueError("oauth2_client_id must be provided")

        # Additional validation for password grant
        if self.oauth2_grant_type == OAuth2GrantType.PASSWORD.value:
            if not self.oauth2_username or not self.oauth2_password:
                raise ValueError("oauth2_username and oauth2_password must be provided for password grant")

        # Validate impersonation configuration
        if self.oauth2_impersonation_enabled:
            if self.oauth2_grant_type == OAuth2GrantType.TOKEN_EXCHANGE.value:
                raise ValueError("oauth2_grant_type cannot be token_exchange when impersonation is enabled")

    def get_qualified_table_name(self, table_name: str, database: Optional[str] = None) -> str:
        """Get fully qualified table name with catalog, database and table.
        
        Args:
            table_name: The name of the table
            database: Optional database name, defaults to default_database if not specified
            
        Returns:
            str: Fully qualified table name in format <catalog_name>.<database>.<table>
        """
        db = database or self.default_database
        return f"{self.catalog_name}.{db}.{table_name}"

    def get_spark_conf(self, warehouse_path: Optional[str] = None) -> dict[str, str]:
        """Get Spark configuration for Nessie catalog.
        
        Args:
            warehouse_path: Optional warehouse path for Iceberg tables
            
        Returns:
            dict[str, str]: Dictionary of Spark configuration options for Nessie.
        """
        # Validate OAuth2 configuration if needed
        if self.authentication_type == "OAUTH2":
            self._validate_oauth2_config()

        prefix = f"spark.sql.catalog.{self.catalog_name}"
        
        conf = {
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
            ),
            f"{prefix}": "org.apache.iceberg.spark.SparkCatalog",
            f"{prefix}.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            f"{prefix}.uri": self.uri,
            f"{prefix}.ref": self.default_branch,
            f"{prefix}.authentication.type": self.authentication_type,
        }

        if warehouse_path:
            conf[f"{prefix}.warehouse"] = warehouse_path

        # Network settings
        if self.request_timeout:
            conf[f"{prefix}.request-timeout"] = str(self.request_timeout)
        if self.connect_timeout:
            conf[f"{prefix}.connect-timeout"] = str(self.connect_timeout)
        if self.max_retries:
            conf[f"{prefix}.max-retries"] = str(self.max_retries)
        if self.retry_delay:
            conf[f"{prefix}.retry-delay"] = str(self.retry_delay)

        # HTTP settings
        if self.max_connections:
            conf[f"{prefix}.max-connections"] = str(self.max_connections)
        if self.keep_alive is not None:
            conf[f"{prefix}.keep-alive"] = str(self.keep_alive).lower()
        if self.compression_enabled is not None:
            conf[f"{prefix}.compression-enabled"] = str(self.compression_enabled).lower()

        # Add authentication details based on type
        if self.authentication_type == "BEARER" and self.token:
            conf[f"{prefix}.authentication.token"] = self.token
        elif self.authentication_type == "OAUTH2":
            # Basic OAuth2 settings
            if self.oauth2_issuer_url:
                conf[f"{prefix}.authentication.oauth2.issuer-url"] = self.oauth2_issuer_url
            if self.oauth2_token_endpoint:
                conf[f"{prefix}.authentication.oauth2.token-endpoint"] = self.oauth2_token_endpoint
            if self.oauth2_auth_endpoint:
                conf[f"{prefix}.authentication.oauth2.auth-endpoint"] = self.oauth2_auth_endpoint
            if self.oauth2_device_auth_endpoint:
                conf[f"{prefix}.authentication.oauth2.device-auth-endpoint"] = self.oauth2_device_auth_endpoint
            if self.oauth2_client_id:
                conf[f"{prefix}.authentication.oauth2.client-id"] = self.oauth2_client_id
            if self.oauth2_client_secret:
                conf[f"{prefix}.authentication.oauth2.client-secret"] = self.oauth2_client_secret
            if self.oauth2_grant_type:
                conf[f"{prefix}.authentication.oauth2.grant-type"] = self.oauth2_grant_type

            # Password grant settings
            if self.oauth2_username:
                conf[f"{prefix}.authentication.oauth2.username"] = self.oauth2_username
            if self.oauth2_password:
                conf[f"{prefix}.authentication.oauth2.password"] = self.oauth2_password

            # Token exchange settings
            if self.oauth2_token_exchange_subject_token:
                conf[f"{prefix}.authentication.oauth2.token-exchange.subject-token"] = self.oauth2_token_exchange_subject_token
            if self.oauth2_token_exchange_requested_token_type:
                conf[f"{prefix}.authentication.oauth2.token-exchange.requested-token-type"] = self.oauth2_token_exchange_requested_token_type

            # Impersonation settings
            if self.oauth2_impersonation_enabled:
                conf[f"{prefix}.authentication.oauth2.impersonation.enabled"] = "true"
                if self.oauth2_impersonation_target:
                    conf[f"{prefix}.authentication.oauth2.impersonation.target"] = self.oauth2_impersonation_target
                if self.oauth2_impersonation_token_endpoint:
                    conf[f"{prefix}.authentication.oauth2.impersonation.token-endpoint"] = self.oauth2_impersonation_token_endpoint

            # Token refresh settings
            if self.oauth2_refresh_enabled is not None:
                conf[f"{prefix}.authentication.oauth2.refresh.enabled"] = str(self.oauth2_refresh_enabled).lower()
            if self.oauth2_refresh_before_expiry:
                conf[f"{prefix}.authentication.oauth2.refresh.before-expiry"] = str(self.oauth2_refresh_before_expiry)
            if self.oauth2_refresh_token:
                conf[f"{prefix}.authentication.oauth2.refresh.token"] = self.oauth2_refresh_token
            if self.oauth2_refresh_token_endpoint:
                conf[f"{prefix}.authentication.oauth2.refresh.token-endpoint"] = self.oauth2_refresh_token_endpoint

        elif self.authentication_type == "AWS" and self.aws_region:
            conf[f"{prefix}.authentication.region"] = self.aws_region

        return conf

    def get_trino_catalog_properties(self) -> dict[str, str]:
        """Get Trino catalog properties for Nessie.
        
        Returns:
            dict[str, str]: Dictionary of Trino catalog properties for Nessie.
        """
        # Validate OAuth2 configuration if needed
        if self.authentication_type == "OAUTH2":
            self._validate_oauth2_config()

        props = {
            "connector.name": "iceberg",
            "iceberg.catalog.type": "nessie",
            "nessie.uri": self.uri,
            "nessie.ref": self.default_branch,
            "nessie.authentication.type": self.authentication_type,
        }

        # Network settings
        if self.request_timeout:
            props["nessie.request-timeout"] = str(self.request_timeout)
        if self.connect_timeout:
            props["nessie.connect-timeout"] = str(self.connect_timeout)
        if self.max_retries:
            props["nessie.max-retries"] = str(self.max_retries)
        if self.retry_delay:
            props["nessie.retry-delay"] = str(self.retry_delay)

        # HTTP settings
        if self.max_connections:
            props["nessie.max-connections"] = str(self.max_connections)
        if self.keep_alive is not None:
            props["nessie.keep-alive"] = str(self.keep_alive).lower()
        if self.compression_enabled is not None:
            props["nessie.compression-enabled"] = str(self.compression_enabled).lower()

        # Add authentication details based on type
        if self.authentication_type == "BEARER" and self.token:
            props["nessie.authentication.token"] = self.token
        elif self.authentication_type == "OAUTH2":
            # Basic OAuth2 settings
            if self.oauth2_issuer_url:
                props["nessie.authentication.oauth2.issuer-url"] = self.oauth2_issuer_url
            if self.oauth2_token_endpoint:
                props["nessie.authentication.oauth2.token-endpoint"] = self.oauth2_token_endpoint
            if self.oauth2_auth_endpoint:
                props["nessie.authentication.oauth2.auth-endpoint"] = self.oauth2_auth_endpoint
            if self.oauth2_device_auth_endpoint:
                props["nessie.authentication.oauth2.device-auth-endpoint"] = self.oauth2_device_auth_endpoint
            if self.oauth2_client_id:
                props["nessie.authentication.oauth2.client-id"] = self.oauth2_client_id
            if self.oauth2_client_secret:
                props["nessie.authentication.oauth2.client-secret"] = self.oauth2_client_secret
            if self.oauth2_grant_type:
                props["nessie.authentication.oauth2.grant-type"] = self.oauth2_grant_type

            # Password grant settings
            if self.oauth2_username:
                props["nessie.authentication.oauth2.username"] = self.oauth2_username
            if self.oauth2_password:
                props["nessie.authentication.oauth2.password"] = self.oauth2_password

            # Token exchange settings
            if self.oauth2_token_exchange_subject_token:
                props["nessie.authentication.oauth2.token-exchange.subject-token"] = self.oauth2_token_exchange_subject_token
            if self.oauth2_token_exchange_requested_token_type:
                props["nessie.authentication.oauth2.token-exchange.requested-token-type"] = self.oauth2_token_exchange_requested_token_type

            # Impersonation settings
            if self.oauth2_impersonation_enabled:
                props["nessie.authentication.oauth2.impersonation.enabled"] = "true"
                if self.oauth2_impersonation_target:
                    props["nessie.authentication.oauth2.impersonation.target"] = self.oauth2_impersonation_target
                if self.oauth2_impersonation_token_endpoint:
                    props["nessie.authentication.oauth2.impersonation.token-endpoint"] = self.oauth2_impersonation_token_endpoint

            # Token refresh settings
            if self.oauth2_refresh_enabled is not None:
                props["nessie.authentication.oauth2.refresh.enabled"] = str(self.oauth2_refresh_enabled).lower()
            if self.oauth2_refresh_before_expiry:
                props["nessie.authentication.oauth2.refresh.before-expiry"] = str(self.oauth2_refresh_before_expiry)
            if self.oauth2_refresh_token:
                props["nessie.authentication.oauth2.refresh.token"] = self.oauth2_refresh_token
            if self.oauth2_refresh_token_endpoint:
                props["nessie.authentication.oauth2.refresh.token-endpoint"] = self.oauth2_refresh_token_endpoint

        elif self.authentication_type == "AWS" and self.aws_region:
            props["nessie.authentication.region"] = self.aws_region

        return props 