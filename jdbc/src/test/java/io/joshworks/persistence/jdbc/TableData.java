package io.joshworks.persistence.jdbc;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class TableData {

    public final int a;
    public final String b;
    public final long c;
    public final double d;
    public final double e;
    public final BigDecimal f;
    public final boolean g;
    public final Timestamp h;
    public final String i;

    public TableData(Integer a, String b, Long c, Double d, Double e, BigDecimal f, Boolean g, Timestamp h, String i) {
        this.a = a;
        this.b = b;
        this.c = c;
        this.d = d;
        this.e = e;
        this.f = f;
        this.g = g;
        this.h = h;
        this.i = i;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("TableData{");
        sb.append("a='").append(a).append('\'');
        sb.append(", b='").append(b).append('\'');
        sb.append(", c=").append(c);
        sb.append(", d=").append(d);
        sb.append(", e=").append(e);
        sb.append(", f=").append(f);
        sb.append(", g=").append(g);
        sb.append(", h=").append(h);
        sb.append(", i='").append(i).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
