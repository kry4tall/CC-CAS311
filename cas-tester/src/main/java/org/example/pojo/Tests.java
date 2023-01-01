package org.example.pojo;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.oracle.webservices.internal.api.databinding.DatabindingMode;

@Table(keyspace = "test",name = "tests")
public class Tests {
    @PartitionKey
    String name;
    String owner;
    String value_1;
    String value_2;
    String value_3;

    @Override
    public String toString() {
        return "Tests{" +
                "name='" + name + '\'' +
                ", owner='" + owner + '\'' +
                ", value_1='" + value_1 + '\'' +
                ", value_2='" + value_2 + '\'' +
                ", value_3='" + value_3 + '\'' +
                '}';
    }
}
