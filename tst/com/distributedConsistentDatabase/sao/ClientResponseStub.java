package com.distributedConsistentDatabase.sao;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;

public class ClientResponseStub extends ClientResponse {

    private Object entity;

    public ClientResponseStub(int status, final Object entity) {
        super(status, null, null, null);
        this.entity = entity;
        // TODO Auto-generated constructor stub
    }

    @SuppressWarnings("unchecked")
	@Override
    public <T> T getEntity(Class<T> c) throws ClientHandlerException, UniformInterfaceException {
        return (T) entity;
    }
}
