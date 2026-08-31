class AppException(Exception):
    """Base exception for application errors."""

    def __init__(self, message: str, status_code: int = 400, headers: dict[str, str] | None = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.headers = headers


class UserAlreadyExistsException(AppException):
    def __init__(self, message: str = "Имя пользователя уже занято"):
        super().__init__(message=message, status_code=409)


class InvalidCredentialsException(AppException):
    def __init__(self, message: str = "Неверное имя пользователя или пароль"):
        super().__init__(message=message, status_code=401, headers={"WWW-Authenticate": "Bearer"})


class InvalidTokenException(AppException):
    def __init__(self, message: str = "Не удалось проверить токен"):
        super().__init__(message=message, status_code=401, headers={"WWW-Authenticate": "Bearer"})


class UserNotFoundException(AppException):
    def __init__(self, message: str = "Пользователь не найден"):
        super().__init__(message=message, status_code=404)


class RateLimitExceededException(AppException):
    def __init__(self, message: str = "Слишком быстро. Одно сообщение в 5 секунд.", retry_after: int = 1):
        super().__init__(message=message, status_code=429, headers={"Retry-After": str(retry_after)})
        self.retry_after = retry_after


class SpamDetectedException(AppException):
    def __init__(self, message: str = "Нельзя отправлять одно и то же сообщение больше двух раз подряд."):
        super().__init__(message=message, status_code=429)


class LockAcquisitionException(AppException):
    def __init__(self, message: str = "Слишком много одновременных запросов"):
        super().__init__(message=message, status_code=429)


class MessageValidationException(AppException):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message=message, status_code=status_code)


class StorageUnavailableException(AppException):
    def __init__(self, message: str = "Хранилище файлов не настроено"):
        super().__init__(message=message, status_code=503)


class PasswordResetException(AppException):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message=message, status_code=status_code)
