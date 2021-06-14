# gql-spansql

[license]: https://github.com/nakatamixi/gql-spansql/blob/master/LICENSE

`gql-spansql` is a command-line tool to convert GraphQL SDL to Google Cloud Spanner table schemas.

# Install
```
go install github.com/nakatamixi/gql-spansql/cmd/gql-spansql
```

# Usage
```
Usage:
  -column-case string
    	snake or lowercamel or uppercamel. if empty no convert.
  -created-column-name string
    	if not empty, add this column as created_at Timestamp column.
  -loose
    	loose type check.
  -s string
    	path to input schama
  -table-case string
    	snake or lowercamel or uppercamel. if empty no convert.
  -updated-column-name string
    	if not empty, add this column as updated_at Timestamp column.
```

# Example
```
cat internal/converter/testdata/spanner_sql.gql
type User {
  userId: ID!
  state: State!
  time: Time!
}

type Item {
  itemId: ID!
}

enum State {
  ENABLED
  DISABLED
}

scalar Time

type Query {
  user(user_id: ID): User
}

type Mutation {
  user(user_id: ID!, state: State): User
}

type Subscription {
  user(user_id: ID): User
}
go run cmd/main.go -s internal/converter/testdata/spanner_sql.gql
CREATE TABLE Item (
  itemId STRING(MAX) NOT NULL,
) PRIMARY KEY(itemId);
CREATE TABLE User (
  userId STRING(MAX) NOT NULL,
  state INT64 NOT NULL,
  time TIMESTAMP NOT NULL,
) PRIMARY KEY(userId);
```
