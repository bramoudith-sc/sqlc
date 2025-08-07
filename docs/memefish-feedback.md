# memefish パッケージのフィードバックと実装知見

sqlc の Cloud Spanner エンジン実装を通じて得られた、memefish パッケージに関する詳細なフィードバックと実装知見をまとめました。

## 実装知見

### AST構造の詳細

#### 基本的な型の扱い
- `IntLiteral.Value` は `string` 型（`int64` ではない） - 大きな数値の精度を保持
- `BinaryOp` は `string` 型で演算子を表現
- `CallExpr.Func` は `*Path` 型（関数名文字列の直接参照ではない）

#### SELECT文の構造
- SELECT結果カラムは `SelectItem` インターフェースを実装
- 単純なカラム参照（`id`, `name`）は `ExprSelectItem` でラップされる
- エイリアス付きカラムは `Alias` 型として表現
- `Star` は直接 `SelectItem` を実装
- `From.Source` に実際のテーブル表現が格納される

#### Query構造の特徴
- `Query` ノードに `OrderBy` と `Limit` が含まれる（`Select` ノードではない）
- これにより、UNION を含むクエリでも適切に ORDER BY と LIMIT を処理できる

### パラメータの扱い
- `@name` 形式のパラメータは `Param` 型で表現
- パラメータ名に `@` プレフィックスは含まれない（`@user_id` → `Name: "user_id"`）
- 位置情報は `Param.Atmark` に `@` の位置が記録される
- Cloud Spanner は `@` パラメータを使用（PostgreSQL の `$1` や MySQL の `?` とは異なる）

### 型システム
- `SchemaType` インターフェースには複数の実装がある：
  - `ScalarSchemaType`: 基本型（INT64, STRING, BOOL など）
  - `SizedSchemaType`: サイズ指定型（STRING(100), BYTES(MAX) など）
  - `ArraySchemaType`: 配列型（ARRAY<INT64> など）
- `ScalarTypeName` は定数として定義
- `SizedSchemaType` は `Max bool` で `STRING(MAX)` のような表現をサポート

### UPDATE文の特殊な構造
- `Update.Updates[].DefaultExpr` は `Default bool` フィールドで DEFAULT キーワードと式を区別
- `DefaultExpr.Expr` が実際の式を含む（`Default == false` の場合）
- `Update.Updates[].Path` にカラム名のパスが格納される

### 位置情報とコメントの扱い
- `SplitRawStatements` は元のテキストの位置を保持するが、コメントとの関連付けは行わない
- `ParseStatements` は簡潔だが、個々のステートメントの元の位置情報が失われる
- **推奨: `memefish.Lexer` を直接使用**することで、より自然で柔軟な実装が可能：
  - `Token.Comments` フィールドにトークンに先行するコメントが含まれる
  - コメントとSQL文の関連付けを正確に行える
  - sqlc のようなメタデータコメント（`-- name: QueryName :many`）の処理に最適
  - 実際に sqlc の Spanner エンジンで採用し、コードが大幅に簡潔化

```go
// Lexer を使ったカスタム実装例
lexer := &memefish.Lexer{
    File: &token.File{
        FilePath: filename,
        Buffer:   sql,
    },
}

for {
    err := lexer.NextToken()
    if err != nil {
        return err
    }
    
    tok := lexer.Token
    
    // tok.Comments にこのトークンに先行するコメントが含まれる
    for _, comment := range tok.Comments {
        commentText := sql[comment.Pos:comment.End]
        // メタデータコメントの処理
    }
    
    if tok.Kind == token.TokenEOF {
        break
    }
}
```

## コード生成ツールでの実践的パターン

### 1. メタデータコメントの処理
Lexer を直接使用する実装が最も効果的です：
```go
// Lexer でステートメントを分割し、コメントを自然に処理
lexer := &memefish.Lexer{
    File: &token.File{
        FilePath: filename,
        Buffer:   content,
    },
}

// tok.Comments でメタデータコメントを取得
// セミコロンでステートメントを分割
// 位置情報を正確に保持
```

従来の `SplitRawStatements` を使う方法では、メタデータコメントを見つけるための複雑なバックトラック処理が必要でした。

### 2. カタログ構築
CREATE TABLE から型情報を抽出し、内部カタログを構築する際は、`SchemaType` の種類に応じた処理が必要

### 3. パラメータ追跡
`Param` ノードを検出し、名前ベースでパラメータを管理（位置ベースではない）

### 4. カラム名解決
`ExprSelectItem` から実際の式を取り出してカラム名を抽出する必要がある

### 5. 予約語チェック（実装済み）
`token.IsKeyword()` 関数が公式の Cloud Spanner 予約語リストと完全に一致することを確認し、sqlc の Spanner エンジンで採用しました：

```go
func (p *Parser) IsReservedKeyword(s string) bool {
    return token.IsKeyword(s)
}
```

特徴：
- 公式ドキュメントの予約語リスト（ALL, AND, ANY, ARRAY, AS, ASC など96個）と完全一致
- INSERT, UPDATE, DELETE, TABLE, INDEX などのコンテキスト依存キーワードは false を返す
- メンテナンスフリー（memefish の更新に自動追従）
- sqlc の Parser インターフェースの一部として `internal/compiler/expand.go` で使用

## ポジティブな点

1. **包括的なAPI**: `ParseStatement`/`ParseStatements`/`ParseDDL`/`ParseDML`/`ParseQuery` による用途別の関数
2. **充実したAST構造**: INTERLEAVE、TTL など Cloud Spanner 特有の構文を完全サポート
3. **優れたエラー処理**: `Error` と `MultiError` による詳細な位置情報付きエラー
4. **便利なユーティリティ**: `SplitRawStatements` による複数文の分割、`ast.Walk`/`ast.WalkMany` による AST 走査
5. **双方向変換**: `Node.SQL()` メソッドによる AST から SQL への逆変換
6. **一貫したインターフェース**: `Pos()`/`End()`/`SQL()` メソッドの統一的な実装
7. **型安全な設計**: marker interface パターンによる compile-time の型チェック

## 改善提案（memefish への機能追加要望）

### 1. パラメータ抽出ユーティリティ
AST から `@param` 形式のパラメータを抽出するユーティリティがあると、ツール開発が容易になります：
```go
// 提案（sqlc の実装で動作確認済み）
type Parameter struct {
    Name     string
    Position token.Pos
}

func ExtractParameters(node Node) []Parameter
```

実際に sqlc で実装してテストした結果、以下のような複雑なクエリでも正確にパラメータを抽出できました：
- 9つの異なるパラメータ（`@user_id`, `@name_pattern`, `@start_date`, `@end_date`, `@status1`, `@status2`, `@status3`, `@limit`, `@offset`）を正しく識別
- 各パラメータの位置情報も保持
- ast.Walk と Visitor パターンを使用した効率的な実装

この機能は ZetaSQL の `GetReferencedParameters` に相当し、クエリ内のパラメータ参照を取得する標準的な機能です。memefish 側で標準機能として提供されれば、より多くのツールで活用できます。

### 2. ドキュメントの充実
- 各AST構造体フィールドの詳細な説明
- `ParseStatement`, `SplitRawStatements`, `ast.Walk` の組み合わせ方の例

## sqlc への統合における今後の拡張ポイント

### analyzer パッケージの実装
PostgreSQL エンジンのように、実際の Cloud Spanner インスタンスに接続してスキーマ情報を取得する機能。INFORMATION_SCHEMA からの型情報取得により、より正確な型マッピングが可能になる。

### 複数の SQL パッケージサポート
- `cloud.google.com/go/spanner` - ネイティブクライアント
- `database/sql` with `go-sql-spanner` - database/sql 互換ドライバ


## 活用可能な追加機能（実装済み）

### ast.Preorder（実装済み）
AST走査の新しいアプローチを sqlc の Spanner エンジンで採用：
```go
// utils.go で実装 - パラメータ抽出を簡潔に
func ExtractParameters(node ast.Node) []Parameter {
    var params []Parameter
    
    for n := range ast.Preorder(node) {
        if param, ok := n.(*ast.Param); ok {
            params = append(params, Parameter{
                Name:     param.Name,
                Position: param.Pos(),
            })
        }
    }
    
    return params
}
```

`ast.Preorder` は Go 1.23+ の `iter.Seq` を使用するイテレータパターンで、従来の Visitor パターンや `ast.Inspect` のコールバックパターンよりも簡潔で読みやすい実装を実現しました。

**ast.Inspect との比較**：
- **Preorder**: より簡潔で Go らしいイテレータスタイル。`break` で早期終了可能
- **Inspect**: コールバックベース。`return false` で特定サブツリーのスキップが可能

パラメータ抽出のような全ノード走査が必要な用途では、`ast.Preorder` の方がシンプルで適切です。

### ParseExpr / ParseType
部分的なSQL要素の解析：
```go
// 計算式の検証
expr, _ := memefish.ParseExpr("", "SAFE.DIVIDE(total, count)")

// 複雑な型の解析
typ, _ := memefish.ParseType("", "ARRAY<STRUCT<id INT64, name STRING>>")
```

### token.QuoteSQL系関数
SQL要素の適切なクォート：
```go
// 識別子のクォート（予約語なら自動的にバッククォート付与）
quoted := token.QuoteSQLIdent("table") // `table`

// 文字列・バイト列のクォート
str := token.QuoteSQLString("O'Reilly") // "O'Reilly"
```

注: QuoteSQLIdent は便利ですが、sqlc では識別子のクォート処理は compiler の expand.go で既に行われているため、現時点では未使用です。

### char.EqualFold（検討済み）
大文字小文字を無視した比較機能ですが、sqlc では以下の方針で実装：
- **識別子**: `convert.go` の `identifier()` 関数で小文字に統一
- **関数名**: そのまま保持し、カタログ検索時（`catalog/public.go`）で小文字比較

この方式により、元のケースを保持しつつ case-insensitive な動作を実現しています。

## まとめ

memefish は Cloud Spanner SQL のパーサーとして非常によく設計されています。実際に sqlc の Spanner エンジンを実装してみて、以下の点が特に優れていることがわかりました：

1. **実用性**: コード生成ツール開発に必要な機能が揃っている
2. **発見可能性**: `go doc` コマンドと Go の型システムにより、ドキュメント化されていない仕様も容易に理解できる
3. **拡張性**: marker interface パターンにより、新しい構文要素の追加が容易
4. **堅牢性**: 詳細なエラー情報と位置情報により、デバッグが容易
5. **豊富なユーティリティ**: ast.Inspect、token.QuoteSQL系、char判定関数など、実装を簡潔にする機能が充実

### sqlc での実装成果

以下の memefish 機能を活用して、効率的な実装を実現：

- **Lexer 直接利用**: ステートメント分割とメタデータコメント処理を大幅に簡潔化
- **token.IsKeyword**: 予約語チェックを 71行から 5行に削減、メンテナンスフリー化
- **ast.Preorder**: Go 1.23+ のイテレータパターンで、パラメータ抽出を最もシンプルに実装
- **大文字小文字処理**: 識別子と関数名で適切に使い分け、case-insensitive 動作を実現

改善提案は主にツール統合をさらに容易にするためのものですが、現状でも十分に実用的なパッケージです。