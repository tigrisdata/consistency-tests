## Changes

* Initial HEAD time is not counted toward convergence time.
  If first HEAD returns correct data then convergence time is 0.
* Removed unnecessary no-cache headers.

## Run individual tests

```bash
BUCKET=<bucket name> AWS_PROFILE=<aws config profile> python3 main.py
```
