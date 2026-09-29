"""The outside world the workflows act on: a pager and a deployer.

Both are in-memory fakes with the shape of real integrations (PagerDuty, a deploy pipeline), so
the example runs anywhere. An application passes its real clients in :class:`OncallDeps`, which
every action receives as ``reaction.deps``.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Page:
    """A page sent to the person on call."""

    service: str
    reason: str


@dataclass
class Pager:
    """Pages the person on call. Keeps every page it sent, by deduplication key."""

    sent: dict[str, Page] = field(default_factory=dict[str, Page])

    @property
    def pages(self) -> list[Page]:
        """Every page sent, oldest first."""
        return list(self.sent.values())

    async def page(self, service: str, reason: str, *, key: str) -> Page:
        """Page the person on call about a service.

        Args:
            service: The service.
            reason: What the person should know.
            key: Deduplicates pages, as real pagers do: a page with a key already used is not
                sent again. Actions pass their run id, so a retried run pages once.
        """
        return self.sent.setdefault(key, Page(service, reason))


@dataclass(frozen=True)
class Rollback:
    """A service rolled back to an earlier version."""

    service: str
    version: str


@dataclass
class Deployer:
    """Rolls services back and checks their health.

    Attributes:
        rollbacks: Every rollback it made, in order.
        unhealthy: How many more health checks each service fails, to simulate a service that
            takes a while to recover.
    """

    rollbacks: list[Rollback] = field(default_factory=list[Rollback])
    unhealthy: dict[str, int] = field(default_factory=dict[str, int])

    async def rollback(self, service: str, version: str) -> Rollback:
        """Roll a service back to ``version``."""
        done = Rollback(service, version)
        self.rollbacks.append(done)
        return done

    async def healthy(self, service: str) -> bool:
        """Whether a service passes its health check."""
        failing = self.unhealthy.get(service, 0)
        self.unhealthy[service] = max(failing - 1, 0)
        return failing == 0


@dataclass
class OncallDeps:
    """What the workflows act on, given to every action as ``reaction.deps``."""

    pager: Pager = field(default_factory=Pager)
    deployer: Deployer = field(default_factory=Deployer)
