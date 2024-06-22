package com.study.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order implements java.io.Serializable{

    private static final long serialVersionUID = 1L;

    private int id;
    private String name;
}
