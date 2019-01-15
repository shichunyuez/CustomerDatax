package com.alibaba.datax.web.service;

import com.alibaba.datax.web.bean.User;
import com.alibaba.datax.web.mapper.UserMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service("userService")
public class UserService extends BaseService<User> {
	@Autowired
	private UserMapper<User> mapper;

	public UserMapper<User> getMapper() {
		return mapper;
	}


}
