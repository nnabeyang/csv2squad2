# csv2squad2

csv2squad2 is a CSV to JSON converter for SQuAD 2.0 format data.

# Usage
- CSV => JSON
```sh
csv2squad2 -i /path/to/file.csv
```
- JSON => CSV
```sh
csv2squad2 -r -i /path/to/file.json
```

# Example

```sh
$ curl -OL https://rajpurkar.github.io/SQuAD-explorer/dataset/dev-v2.0.json
$ csv2squad2 -r -i dev-v2.0.json
$ csv2squad2 -i out.csv
$ diff -u <(jq -S . dev-v2.0.json) <(jq -S . out.json)
```

# License
MIT

# Author
[Noriaki Watanabe@nnabeyang](https://twitter.com/nnabeyang)
