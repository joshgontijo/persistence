package io.joshworks.snappy.extras.jdbc;

import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class Rows {

    private final LinkedList<Row> items = new LinkedList<>();

    static Rows fromResultSet(ResultSet rs) {
        Rows rows = new Rows();
        try {
            while (rs.next()) {
                rows.add(Row.fromResultSet(rs));
            }
        } catch (Exception e) {
            throw new JdbcException(e);
        }
        return rows;
    }

    private void add(Row row) {
        items.add(row);
    }

    private Optional<Row> get(int index) {
        if(items.isEmpty() || index >= items.size()) {
            return Optional.empty();
        }
        return Optional.of(items.get(index));
    }

    public Optional<Row> first() {
        return get(0);
    }

    public Optional<Row> last() {
        return get(items.size() - 1);
    }

    public int size() {
        return items.size();
    }

    public Stream<Row> stream() {
        return items.stream();
    }

    public void forEach(Consumer<Row> consumer) {
        stream().forEach(consumer);
    }

}
