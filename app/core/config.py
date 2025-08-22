from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    KASPI_TOKEN: str = ""

    TZ: str = "Asia/Almaty"
    DAY_CUTOFF: str = "20:00"
    PACK_LOOKBACK_DAYS: int = 3

    AMOUNT_FIELDS: str = "totalPrice"
    AMOUNT_DIVISOR: int = 1
    CHUNK_DAYS: int = 7

    DATE_FIELD_DEFAULT: str = "creationDate"
    DATE_FIELD_OPTIONS: str = "creationDate,plannedShipmentDate,plannedDeliveryDate,shipmentDate,deliveryDate"

    HOST: str = "0.0.0.0"
    PORT: int = 8899
    DEBUG: bool = True

    @property
    def amount_fields(self):
        return [x.strip() for x in self.AMOUNT_FIELDS.split(",") if x.strip()]

    @property
    def date_field_options(self):
        return [x.strip() for x in self.DATE_FIELD_OPTIONS.split(",") if x.strip()]

settings = Settings()
