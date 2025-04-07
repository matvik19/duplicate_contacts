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


class NetworkError(Exception):
    """Сетевая ошибка при выполнении запроса."""

    pass


class ValidationError(Exception):
    """Ошибка валидации входных данных."""

    pass


class ProcessingError(Exception):
    """Общая ошибка обработки данных."""

    pass


class TokenError(Exception):
    """Общая ошибка обработки данных."""

    pass
