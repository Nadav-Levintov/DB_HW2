package techflix;

import org.junit.Test;
import techflix.business.Movie;
import techflix.business.MovieRating;
import techflix.business.ReturnValue;
import techflix.business.Viewer;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static techflix.business.ReturnValue.OK;

public class SimpleTest extends  AbstractTest {

    @Test
    public void simpleTestCreateUser()
    {
        /* Init basic viewer and movies */
        Viewer viewer1 = new Viewer();
        viewer1.setName("viewer1");
        viewer1.setId(1);
        Viewer viewer2 = new Viewer();
        viewer2.setName("viewer2");
        viewer2.setId(2);
        Viewer viewer3 = new Viewer();
        viewer3.setName("viewer3");
        viewer3.setId(3);
        Movie m1 = new Movie();
        m1.setId(1);
        m1.setName("1N");
        m1.setDescription("1D");
        Movie m2 = new Movie();
        m2.setId(2);
        m2.setName("2N");
        m2.setDescription("2D");
        Movie m3 = new Movie();
        m3.setId(3);
        m3.setName("3N");
        m3.setDescription("3D");
        Movie m4 = new Movie();
        m4.setId(4);
        m4.setName("4N");
        m4.setDescription("4D");


        ReturnValue actual = Solution.createViewer(viewer1);
        assertEquals(OK, actual);
        actual = Solution.createViewer(viewer2);
        assertEquals(OK, actual);
        actual = Solution.createViewer(viewer3);
        assertEquals(OK, actual);
        actual = Solution.createMovie(m1);
        assertEquals(OK, actual);
        actual = Solution.createMovie(m2);
        assertEquals(OK, actual);
        actual = Solution.createMovie(m3);
        assertEquals(OK, actual);
        actual = Solution.createMovie(m4);
        assertEquals(OK, actual);

        /* Init views */
        actual = Solution.addView(1,1);
        assertEquals(OK, actual);
        /*actual = Solution.addView(1,2);
        assertEquals(OK, actual);
        actual = Solution.addView(1,3);
        assertEquals(OK, actual);
        actual = Solution.addView(1,4);
        assertEquals(OK, actual);
        */
        /* Test getSimilarViews*/
        ArrayList<Integer> res = Solution.getSimilarViewers(1);
        assertEquals(res.isEmpty(),Boolean.TRUE);

        /*actual = Solution.addView(2,1);
        assertEquals(OK, actual);
        actual = Solution.addView(2,2);
        assertEquals(OK, actual);*/
        res = Solution.getSimilarViewers(1);
        assertEquals(res.isEmpty(),Boolean.TRUE);

        actual = Solution.addView(2,3);
        assertEquals(OK, actual);
        actual = Solution.addView(2,4);
        assertEquals(OK, actual);
        //res = Solution.getSimilarViewers(1);
        //assertEquals(1,res.size());

        actual = Solution.addView(3,1);
        assertEquals(OK, actual);
        actual = Solution.addView(3,2);
        assertEquals(OK, actual);

        /*actual = Solution.addView(3,3);
        assertEquals(OK, actual);
        actual = Solution.addView(3,4);
        assertEquals(OK, actual);*/
        //res = Solution.getSimilarViewers(1);
        //assertEquals(2,res.size());

        actual = Solution.addMovieRating(1, 1,MovieRating.LIKE);
        assertEquals(OK, actual);
        actual = Solution.addMovieRating(3,1, MovieRating.LIKE);
        assertEquals(OK, actual);
        actual = Solution.addMovieRating(2,3, MovieRating.LIKE);
        assertEquals(OK, actual);

        res = Solution.getSimilarViewers(1);
        assertEquals(Boolean.TRUE,res.size() < 11);
        assertEquals(Boolean.TRUE,res.size() > 0);


    }

}
