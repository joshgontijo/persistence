package io.joshworks.snappy.extras.jdbc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Row {

    private final Map<String, Object> byName = new HashMap<>();
    private final Object[] byIndex;

    private Row(ResultSet rs) {
        try {

            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            this.byIndex = new Object[cols];
            for (int i = 1; i <= cols; i++) {
                Object value = rs.getObject(i);
                byIndex[i - 1] = value;
                byName.put(meta.getColumnName(i), value);
            }


        } catch (SQLException e) {
            throw new JdbcException("Could not read row", e);
        }
    }

    static Row fromResultSet(ResultSet rs) {
        return new Row(rs);
    }


    public Object value(String name) {
        return byName.get(name);
    }

    public Object value(int index) {
        return byIndex[index];
    }

    public Long asLong(String name) {
        Object val = byName.get(name);
        if (val == null) {
            return null;
        }
        return (Long) val;
    }

    public Float asFloat(String name) {
        Object val = byName.get(name);
        if (val == null) {
            return null;
        }
        return (Float) val;
    }

    public Double asDouble(String name) {
        Object val = byName.get(name);
        if (val == null) {
            return null;
        }
        return (Double) val;
    }

    public BigDecimal asBigDecimal(String name) {
        Object val = byName.get(name);
        if (val == null) {
            return null;
        }
        return (BigDecimal) val;
    }

    public Short asShort(String name) {
        Object val = byName.get(name);
        if (val == null) {
            return null;
        }
        return (Short) val;
    }

    public Byte asByte(String name) {
        Object val = byName.get(name);
        if (val == null) {
            return null;
        }
        return (Byte) val;
    }

    public Boolean asBoolean(String name) {
        Object val = byName.get(name);
        if (val == null) {
            return null;
        }
        return (Boolean) val;
    }

    public Character asChar(String name) {
        Object val = byName.get(name);
        if (val == null) {
            return null;
        }
        return (Character) val;
    }

    public Integer asInt(String name) {
        Object val = byName.get(name);
        if (val == null) {
            return null;
        }
        return (Integer) val;
    }

    public Integer asInt(int index) {
        Object val = byIndex[index];
        if (val == null) {
            return null;
        }
        return (Integer) val;
    }

    public Float asFloat(int index) {
        Object val = byIndex[index];
        if (val == null) {
            return null;
        }
        return (Float) val;
    }

    public Double asDouble(int index) {
        Object val = byIndex[index];
        if (val == null) {
            return null;
        }
        return (Double) val;
    }

    public BigDecimal asBigDecimal(int index) {
        Object val = byIndex[index];
        if (val == null) {
            return null;
        }
        return (BigDecimal) val;
    }

    public Long asLong(int index) {
        Object val = byIndex[index];
        if (val == null) {
            return null;
        }
        return (Long) val;
    }

    public Short asShort(int index) {
        Object val = byIndex[index];
        if (val == null) {
            return null;
        }
        return (Short) val;
    }

    public Byte asByte(int index) {
        Object val = byIndex[index];
        if (val == null) {
            return null;
        }
        return (Byte) val;
    }

    public Boolean asBoolean(int index) {
        Object val = byIndex[index];
        if (val == null) {
            return null;
        }
        return (Boolean) val;
    }

    public Character asChar(int index) {
        Object val = byIndex[index];
        if (val == null) {
            return null;
        }
        return (Character) val;
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, Object> entry : byName.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    public <T> T as(Class<T> type) {
        Constructor<T> constructor = getConstructor(type);
        return newInstance(constructor);
    }

    private <T> Constructor<T>  getConstructor(Class<T> type) {
        Class[] types = Stream.of(byIndex).map(Object::getClass).toArray(Class[]::new);
        try {
            return type.getConstructor(types);
        } catch (Exception e) {
            StringBuilder consTypes = new StringBuilder();
            for (Constructor ctor : type.getConstructors()) {
                Class<?>[] pType  = ctor.getParameterTypes();
                consTypes.append("[").append(Arrays.toString(pType)).append("]\n");
            }
            String message = MessageFormat.format("Expected constructor:\n[{0}]\nAvailable constructors:\n{1}", Arrays.toString(types), consTypes);
            throw new RuntimeException(message);
        }
    }

    private <T> T newInstance(Constructor<T> constructor) {
        try {
            return constructor.newInstance(byIndex);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Could not invoke constructor", e);
        }
    }

    @Override
    public String toString() {
        String joinedString = asMap().entrySet().stream()
                .map(kv -> kv.getKey() + "=" + String.valueOf(kv.getValue()))
                .collect(Collectors.joining(", "));
        return "[" + joinedString + "]";
    }
}
