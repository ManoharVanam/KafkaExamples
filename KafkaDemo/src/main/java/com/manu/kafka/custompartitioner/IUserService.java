package com.manu.kafka.custompartitioner;

import java.util.List;

interface IUserService {
    public Integer findUserId(String userName);
    public List<String> findAllUsers();
}
