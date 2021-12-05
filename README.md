# Turtle formatter plugin for Embulk

Turtle formatter plugin for Embulk, output RDF with tutle format.

## Overview

* **Plugin type**: formatter

## Configuration

- **base**: base uri (string, required)
- **subject_column**: subject column name (string, required)
- **columns**: columns, name and predicate (map-list, required)

## Example

```yaml
out:
  type: any output input plugin type
  formatter:
    type: turtle
    base: http://example.com/ttl/
    subject_column: 'id'
    columns:
      - {name: 'account', predicate: 'http://example.com/ttl/type#account'}
      - {name: 'time', predicate: 'http://example.com/ttl/type#time'}
      - {name: 'purchase', predicate: 'http://example.com/ttl/type#purchase'}
      - {name: 'comment', predicate: 'http://example.com/ttl/type#comment'}
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
