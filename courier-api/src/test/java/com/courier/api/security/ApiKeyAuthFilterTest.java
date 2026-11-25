package com.courier.api.security;

import com.courier.common.security.ApiKeyAuthFilter;
import com.courier.common.security.ApiKeyProperties;
import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/*
 * Unit tests for the filter on its own: what it puts in the SecurityContext,
 * and that it never short-circuits the chain. The HTTP-level behaviour
 * (401/403) is covered by ApiSecurityTest.
 */
@DisplayName("ApiKeyAuthFilter")
class ApiKeyAuthFilterTest {

    private ApiKeyAuthFilter filter;
    private FilterChain chain;
    private MockHttpServletRequest request;
    private MockHttpServletResponse response;

    @BeforeEach
    void setUp() {
        ApiKeyProperties.Client client = new ApiKeyProperties.Client();
        client.setKey("client-secret");
        client.setClientId("client-a");
        client.setRole("CLIENT");

        ApiKeyProperties.Client noKey = new ApiKeyProperties.Client();
        noKey.setKey("");               // misconfigured: must never match anything
        noKey.setClientId("ghost");
        noKey.setRole("ADMIN");

        ApiKeyProperties props = new ApiKeyProperties();
        props.setClients(List.of(noKey, client));

        filter = new ApiKeyAuthFilter(props);
        chain = mock(FilterChain.class);
        request = new MockHttpServletRequest("POST", "/api/v1/notifications");
        response = new MockHttpServletResponse();
        SecurityContextHolder.clearContext();
    }

    @AfterEach
    void clear() {
        SecurityContextHolder.clearContext();
    }

    @Test
    @DisplayName("Valid key: principal is the clientId, role is prefixed ROLE_, credentials are null")
    void validKeyAuthenticates() throws Exception {
        request.addHeader(ApiKeyAuthFilter.HEADER, "client-secret");

        filter.doFilter(request, response, chain);

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        assertThat(auth).isNotNull();
        assertThat(auth.getName()).isEqualTo("client-a");
        assertThat(auth.getCredentials()).isNull();          // the secret goes no further
        assertThat(auth.getAuthorities()).extracting(Object::toString)
                .containsExactly("ROLE_CLIENT");
        verify(chain).doFilter(request, response);
    }

    @Test
    @DisplayName("Unknown key: no authentication is set, but the chain still continues")
    void unknownKeyLeavesContextEmpty() throws Exception {
        request.addHeader(ApiKeyAuthFilter.HEADER, "wrong");

        filter.doFilter(request, response, chain);

        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
        verify(chain).doFilter(request, response);
    }

    @Test
    @DisplayName("No header: no authentication, chain continues (so permitAll paths still work)")
    void missingHeaderLeavesContextEmpty() throws Exception {
        filter.doFilter(request, response, chain);

        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
        verify(chain).doFilter(request, response);
    }

    @Test
    @DisplayName("A client configured with an empty key can never be matched")
    void emptyConfiguredKeyNeverMatches() throws Exception {
        request.addHeader(ApiKeyAuthFilter.HEADER, "x");   // non-blank, matches nothing
        filter.doFilter(request, response, chain);
        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
    }
}
