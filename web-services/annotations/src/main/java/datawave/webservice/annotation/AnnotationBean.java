package datawave.webservice.annotation;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

@Path("/Annotations")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class AnnotationBean {

    @GET
    @Path("{id}/types")
    @Produces("application/json")
    public Response getAllAnnotationTypes(@PathParam("id") String id, @QueryParam("idType") String idType) {
        Set<String> types = new HashSet<>();
        // TODO
        return Response.ok(types).build();
    }

    @GET
    @Path("/{id}")
    @Produces("application/json")
    public Response getAnnotationsFor(@PathParam("id") String id, @QueryParam("idType") String idType) {
        // TODO

        return Response.ok().build();
    }

    @GET
    @Path("/{id}/type/{annotationType}")
    @Produces("application/json")
    public Response getAnnotationsByType(@PathParam("id") String id, @QueryParam("idType") String idType, @PathParam("annotationType") String annotationType) {
        // TODO

        return Response.ok().build();
    }

    @GET
    @Path("/{id}/annotation/{annotationId}")
    @Produces("application/json")
    public Response getAnnotation(@PathParam("id") String id, @QueryParam("idType") String idType, @PathParam("annotationId") String annotationId) {
        // TODO

        return Response.ok().build();
    }

    @PUT
    @Path("/{id}/annotation/{annotationId}")
    @Produces("application/json")
    public Response updateAnnotation(@PathParam("id") String id, @QueryParam("idType") String idType, @PathParam("annotationId") String annotationId) {
        // TODO return the updated annotation

        return Response.ok().build();
    }

    @GET
    @Path("/{id}/annotation/{annotationId}/segment/{segmentId}")
    @Produces("application/json")
    public Response getAnnotationSegment(@PathParam("id") String id, @QueryParam("idType") String idType, @PathParam("annotationId") String annotationId,
                    @PathParam("segmentId") String segmentId) {
        // TODO

        return Response.ok().build();
    }

    @POST
    @Path("/{id}/annotation/{annotationId}/segment")
    @Consumes("application/json")
    @Produces("application/json")
    public Response addSegment(@PathParam("id") String id, @QueryParam("idType") String idType, @PathParam("annotationId") String annotationId) {
        // TODO return the new segment

        return Response.ok().build();
    }

    @PUT
    @Path("/{id}/annotation/{annotationId}/segment/{segmentId}")
    @Consumes("application/json")
    @Produces("application/json")
    public Response updateSegment(@PathParam("id") String id, @QueryParam("idType") String idType, @PathParam("annotationId") String annotationId,
                    @PathParam("segmentId") String segmentId) {
        // TODO

        return Response.ok().build();
    }
}
