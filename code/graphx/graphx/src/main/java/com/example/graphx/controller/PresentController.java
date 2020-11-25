package com.example.graphx.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class PresentController {
    @RequestMapping("/graph")
    public String graph() {
        return "graph";
    }

    @RequestMapping("/rank")
    public String rank() {
        return "rank";
    }

    @RequestMapping("/distribute")
    public String distribute() {
        return "distribute";
    }

    @RequestMapping("/jump")
    public String jump() {
        return "twoJump";
    }

}
