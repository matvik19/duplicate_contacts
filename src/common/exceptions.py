class DuplicateProcessingError(Exception):
    """Базовое исключение для ошибок обработки дублей."""

    pass


class SettingsNotFoundError(DuplicateProcessingError):
    """Ошибка: настройки дублей не найдены."""

    pass


class MergeDisabledError(DuplicateProcessingError):
    """Ошибка: слияние отключено в настройках."""

    pass


class AmoCRMServiceError(DuplicateProcessingError):
    """Ошибка в работе с AmoCRM API."""

    pass


class DatabaseError(DuplicateProcessingError):
    """Ошибка при работе с базой данных."""

    pass
