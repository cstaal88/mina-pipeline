"""Small helper to initialise a MediaCloud client safely.

This module loads environment variables (via dotenv when available),
looks up a MediaCloud API key and creates a client object. It avoids
importing notebook-only helpers unconditionally and provides a small
helper function to get the client for other scripts.

If no API key is found, `get_client()` will return None and callers
should handle that case (or the helper `require_client()` will raise
with an explanatory message).
"""

from __future__ import annotations

import os
import sys
import datetime as dt
from importlib.metadata import version

try:
	# dotenv is optional — load local .env if present
	from dotenv import load_dotenv
	load_dotenv()
except Exception:
	# not fatal; environment variables may already be set
	pass

_MC_ENV_NAMES = ("MEDIACLOUD_API_KEY", "MC_API_KEY", "MY_API_KEY")

def _find_api_key() -> str | None:
	for name in _MC_ENV_NAMES:
		val = os.getenv(name)
		if val:
			return val
	return None


_mc = None
_mc_version = None

try:
	import mediacloud.api as mc_api

	_mc_version = None
	try:
		_mc_version = version("mediacloud")
	except Exception:
		# version lookup is best-effort
		_mc_version = None

	_api_key = _find_api_key()
	if _api_key:
		# Most installs expose SearchApi — use it to run search/words queries.
		try:
			if hasattr(mc_api, 'SearchApi'):
				_mc = mc_api.SearchApi(_api_key)
			else:
				# last-resort: try to find any callable factory in the module
				for name in dir(mc_api):
					attr = getattr(mc_api, name)
					if callable(attr):
						try:
							_mc = attr(_api_key)
							break
						except Exception:
							continue
		except Exception:
			_mc = None
	else:
		_mc = None
except Exception:
	mc_api = None
	_mc = None


def get_client():
	"""Return the initialised MediaCloud client or None if unavailable."""
	return _mc


def require_client():
	"""Return the client or raise a helpful RuntimeError if missing."""
	if _mc is None:
		names = ", ".join(_MC_ENV_NAMES)
		raise RuntimeError(
			f"MediaCloud client not initialised. Set one of: {names} in environment."
		)
	return _mc


def client_version() -> str | None:
	"""Return detected mediacloud package version if available."""
	return _mc_version


def example_fetch_top_terms():
    """Fetch top terms for a query using the MediaCloud API."""
    client = get_client()
    if client is None:
        print("No MediaCloud client available. Ensure MEDIACLOUD_API_KEY is set.")
        return

    query = '"climate change"'  # Example query
    start_date = dt.date(2023, 11, 1)
    end_date = dt.date(2023, 12, 1)
    source_ids = [2]  # Washington Post (example from tutorial)

    try:
        print(f"Fetching top terms for query: {query} from {start_date} to {end_date}")
        results = client.words(query, start_date=start_date, end_date=end_date, source_ids=source_ids)
        print("Results:")
        print(results)
    except Exception as e:
        print("Error fetching top terms:", e)


def example_fetch_news_articles():
    """Fetch specific news articles (URLs) matching a query."""
    client = require_client()
    query = "climate change"
    start_date = "2025-09-23"
    end_date = "2025-10-23"

    print(f"Fetching news articles for query: \"{query}\" from {start_date} to {end_date}")

    all_stories = []
    pagination_token = None
    more_stories = True

    try:
        while more_stories:
            page, pagination_token = client.story_list(
                q=query,
                fq=f"publish_date:[{start_date}T00:00:00Z TO {end_date}T23:59:59Z]",
                limit=100,  # Updated from 'rows' to 'limit'
                pagination_token=pagination_token
            )
            all_stories.extend(page)
            more_stories = pagination_token is not None

        for story in all_stories[:10]:  # Display only the first 10 stories for brevity
            print(f"Title: {story['title']}")
            print(f"URL: {story['url']}")
            print("---")

        print(f"Retrieved {len(all_stories)} matching stories.")

    except Exception as e:
        print(f"Error fetching news articles: {e}")


if __name__ == "__main__":
	# Simple smoke test when run directly
	print("mcloud helper — lightweight MediaCloud initialiser")
	print("time:", dt.datetime.now(dt.UTC).isoformat() + "Z")  # Updated to use timezone-aware datetime
	print("mediacloud package:", client_version() or "(not installed)")
	import argparse

	parser = argparse.ArgumentParser(description="mcloud helper — demo runner")
	parser.add_argument("--demo", action="store_true", help="run a small 'words' demo (requires API key)")
	args = parser.parse_args()

	if args.demo:
		# run a tiny demo that mimics the notebook example: words() for "climate change"
		client = get_client()
		if client is None:
			print("No MediaCloud client available. Make sure MEDIACLOUD_API_KEY is exported in your shell.")
			sys.exit(2)

		# build demo parameters (using a safe short date range)
		try:
			start_date = dt.date.today() - dt.timedelta(days=30)
			end_date = dt.date.today()
		except Exception:
			start_date, end_date = None, None

		query = '"climate change"'  # mirror tutorial: quoted phrase
		print(f"Running demo words() for query: {query} from {start_date} to {end_date}")
		try:
			# The MediaCloud Python client exposes a 'words' method on the search API.
			# There are different client wrappers; try common attribute names safely.
			if hasattr(_mc, 'words'):
				results = _mc.words(query, start_date=start_date, end_date=end_date, source_ids=[2])
			elif hasattr(_mc, 'search') and hasattr(_mc.search, 'words'):
				results = _mc.search.words(query, start_date=start_date, end_date=end_date, source_ids=[2])
			else:
				# fallback: try to call via a generic api.SearchApi if available
				try:
					import mediacloud.api as _mc_api
					search = _mc_api.SearchApi(_find_api_key())
					results = search.words(query, start_date, end_date, source_ids=[2])
				except Exception as e:  # pragma: no cover - runtime fallback
					print("Unable to call words() on the installed mediacloud client:", e)
					sys.exit(3)

			# print a short summary of results
			if results is None:
				print("No results returned")
			else:
				# results typically contain 'terms' or a list; print top 10
				import json
				print(json.dumps(results if isinstance(results, dict) else {'results': results}, indent=2)[:4000])
				print("\n...demo complete (output truncated)")
		except Exception as e:
			print("Demo call failed:", e)
			raise
		else:
			example_fetch_top_terms()
			example_fetch_news_articles()
	else:
		client = get_client()
		if client is None:
			print(
				"No MediaCloud client available. Set MEDIACLOUD_API_KEY (or MC_API_KEY / MY_API_KEY) in your environment."
			)
			sys.exit(1)
		else:
			print("MediaCloud client initialised — ready to use.")