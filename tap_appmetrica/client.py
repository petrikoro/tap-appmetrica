"""REST client handling, including AppMetricaStream base class."""

from __future__ import annotations

import backoff
import pendulum
from pathlib import Path
from typing import Any, Iterable, Generator

import requests
from memoization import cached
from singer_sdk import metrics
from singer_sdk.streams import RESTStream
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.authenticators import SimpleAuthenticator

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class AppMetricaStream(RESTStream):
    """AppMetricaStream stream class."""

    _LOG_REQUEST_METRIC_URLS = True
    extra_retry_statuses = [202] + RESTStream.extra_retry_statuses

    def backoff_wait_generator(self) -> Generator[float, None, None]:
        """
        The wait generator used by the backoff decorator on request failure.

        RETURNS:
            The wait generator
        """

        return backoff.constant(120)

    def backoff_max_tries(self) -> int:
        """
        The number of attempts before giving up when retrying requests.

        RETURNS:
            Number of max retries.
        """

        return 30

    @property
    def timeout(self) -> int:
        """
        Return the request timeout limit in seconds.

        The default timeout is 300 seconds, or as defined by DEFAULT_REQUEST_TIMEOUT.

        RETURNS:
            The request timeout limit as number of seconds.
        """

        return 300

    @property
    def url_base(self) -> str:
        """Return the base url, e.g. https://api.mysite.com/v3/."""

        return "https://api.appmetrica.yandex.ru"

    @property
    @cached
    def authenticator(self) -> SimpleAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return SimpleAuthenticator(
            self, {"Authorization": f"OAuth {self.config['token']}"}
        )

    @property
    def requests_session(self) -> requests.Session:
        if not self._requests_session:
            self._requests_session = requests.Session()
            self._requests_session.stream = True
        return self._requests_session

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}

        params["application_id"] = self.config["application_id"]
        params["date_dimension"] = "receive"

        params["date_since"] = next_page_token.to_datetime_string()
        params["date_until"] = (
            next_page_token + pendulum.duration(days=self.config["chunk_days"])
        ).to_datetime_string()

        if limit := self.config.get("limit") is not None:
            params["limit"] = limit

        params["fields"] = ",".join(self.fields)

        return params

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        now = pendulum.now(tz=self.config.get("time_zone"))
        replication_key_value = pendulum.parse(
            self.get_starting_replication_key_value(context)
        )

        decorated_request = self.request_decorator(self._request)

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context

            while replication_key_value < now:
                prepared_request = self.prepare_request(
                    context,
                    next_page_token=replication_key_value,
                )

                resp = decorated_request(prepared_request, context)
                request_counter.increment()
                self.update_sync_costs(prepared_request, resp, context)

                yield from self.parse_response(resp)

                self.finalize_state_progress_markers()
                self._write_state_message()

                replication_key_value += pendulum.duration(
                    days=self.config["chunk_days"]
                )

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """

        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
