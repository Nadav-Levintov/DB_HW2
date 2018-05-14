package techflix;

import techflix.business.Movie;
import techflix.business.MovieRating;
import techflix.business.ReturnValue;
import techflix.business.Viewer;
import techflix.data.DBConnector;
import techflix.data.PostgresSQLErrorCodes;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Solution {

    static List<Viewer> parse_viewers(ResultSet resultSet)
    {
        ArrayList<Viewer> viewers = new ArrayList<>();

        try {
            while(resultSet.next())
            {
                Viewer viewer = new Viewer();

                viewer.setId(resultSet.getInt(1));
                viewer.setName(resultSet.getString(2));

                viewers.add(viewer);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return viewers;
    }

    static List<Movie> parse_movies(ResultSet resultSet)
    {
        ArrayList<Movie> movies = new ArrayList<>();

        try {
            while(resultSet.next())
            {
                Movie movie = new Movie();

                movie.setId(resultSet.getInt(1));
                movie.setName(resultSet.getString(2));
                movie.setDescription(resultSet.getString(3));

                movies.add(movie);

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return movies;
    }

    public static void createTables()
    {
        // Open connection
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            // Create TYPE MovieRating
            pstmt = connection.prepareStatement("CREATE TYPE public.\"MovieRating\" AS ENUM\n" +
                    "('LIKE', 'DISLIKE')");
            pstmt.execute();

            // Create Viewer table
            pstmt = connection.prepareStatement("CREATE TABLE public.viewer\n" +
                    "(\n" +
                    "    viewer_id integer NOT NULL,\n" +
                    "    name text COLLATE pg_catalog.\"default\" NOT NULL,\n" +
                    "    CONSTRAINT \"Viewer_pkey\" PRIMARY KEY (viewer_id),\n" +
                    "    CONSTRAINT id_pos CHECK (viewer_id > 0)\n" +
                    ")");
            pstmt.execute();

            // Create Movie table
            pstmt = connection.prepareStatement("CREATE TABLE public.movie\n" +
                    "(\n" +
                    "    movie_id integer NOT NULL,\n" +
                    "    name text COLLATE pg_catalog.\"default\" NOT NULL,\n" +
                    "    description text COLLATE pg_catalog.\"default\" NOT NULL,\n" +
                    "    CONSTRAINT movie_pkey PRIMARY KEY (movie_id),\n" +
                    "    CONSTRAINT pos_id CHECK (movie_id > 0)\n" +
                    ")");
            pstmt.execute();

            // Create viewed_ranked
            pstmt = connection.prepareStatement("CREATE TABLE public.viewed_liked\n" +
                    "(\n" +
                    "    viewer_id integer NOT NULL,\n" +
                    "    movie_id integer NOT NULL,\n" +
                    "    liked \"MovieRating\",\n" +
                    "    CONSTRAINT key PRIMARY KEY (viewer_id, movie_id),\n" +
                    "    CONSTRAINT for_movie FOREIGN KEY (movie_id)\n" +
                    "        REFERENCES public.movie (movie_id) MATCH SIMPLE\n" +
                    "        ON UPDATE NO ACTION\n" +
                    "        ON DELETE CASCADE\n" +
                    "        NOT VALID,\n" +
                    "    CONSTRAINT for_viewer FOREIGN KEY (viewer_id)\n" +
                    "        REFERENCES public.viewer (viewer_id) MATCH SIMPLE\n" +
                    "        ON UPDATE NO ACTION\n" +
                    "        ON DELETE CASCADE \n" +
                    ")");
            pstmt.execute();

        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }


    public static void clearTables()
    {
        // Open connection
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            // Clear Viewer table
            pstmt = connection.prepareStatement("DELETE FROM public.viewer\n");
            pstmt.execute();

            // Clear Movie table
            pstmt = connection.prepareStatement("DELETE FROM public.movie\n");
            pstmt.execute();

            // Create viewed_ranked
            pstmt = connection.prepareStatement("DELETE FROM public.viewed_liked\n");
            pstmt.execute();

        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }


    public static void dropTables()
    {
// Open connection
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            // Drop viewed_ranked
            pstmt = connection.prepareStatement("DROP TABLE public.viewed_liked\n");
            pstmt.execute();

            // Drop Viewer table
            pstmt = connection.prepareStatement("DROP TABLE public.viewer\n");
            pstmt.execute();

            // Drop Movie table
            pstmt = connection.prepareStatement("DROP TABLE public.movie\n");
            pstmt.execute();

            // Drop TYPE MovieRating
            pstmt = connection.prepareStatement("DROP TYPE public.\"MovieRating\"");
            pstmt.execute();

        } catch (SQLException e) {
            System.out.println(e.getSQLState());
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }


    public static ReturnValue createViewer(Viewer viewer)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO public.viewer" +
                    " VALUES (?, ?)");
            pstmt.setInt(1, viewer.getId());
            pstmt.setString(2, viewer.getName());

            pstmt.execute();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue() ||
                    Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                    return ReturnValue.BAD_PARAMS;
            }else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;
            }else
            {
                // Unexcpected exceptions was thrown
                return ReturnValue.ERROR;
            }

            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    public static ReturnValue deleteViewer(Viewer viewer)
    {
        //Open connection
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        Integer res = 0;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM public.viewer " +
                            "where viewer_id = ?");
            pstmt.setInt(1,viewer.getId());
            res = pstmt.executeUpdate();
        } catch (SQLException e) {
                // Unexcpected exceptions was thrown
                return ReturnValue.ERROR;
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }


        if( res.equals(0))
        {
            return  ReturnValue.NOT_EXISTS;
        }
        return ReturnValue.OK;
    }

    public static ReturnValue updateViewer(Viewer viewer)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        int res = 0;
        try {
            pstmt = connection.prepareStatement(
                    "UPDATE viewer " +
                            "SET name = ? " +
                            "where viewer_id = ?");
            pstmt.setInt(2,viewer.getId());
            pstmt.setString(1, viewer.getName());
            res = pstmt.executeUpdate();
        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue() ||
                    Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }else
            {
                // Unexcpected exceptions was thrown
                return ReturnValue.ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        if( res == 0)
            return ReturnValue.NOT_EXISTS;
        return ReturnValue.OK;
    }

    public static Viewer getViewer(Integer viewerId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        List<Viewer> viewers = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement("SELECT * FROM viewer " +
            "WHERE viewer_id = ? ");
            pstmt.setInt(1,viewerId);
            ResultSet results = pstmt.executeQuery();
            viewers = parse_viewers(results);
            results.close();

        } catch (SQLException e) {
            return Viewer.badViewer();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        if(viewers.isEmpty())
        {
            return Viewer.badViewer();
        }

        return viewers.get(0);
    }


    public static ReturnValue createMovie(Movie movie)
    {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO public.movie" +
                    " VALUES (?, ?, ?)");
            pstmt.setInt(1, movie.getId());
            pstmt.setString(2, movie.getName());
            pstmt.setString(3, movie.getDescription());

            pstmt.execute();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue() ||
                    Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;
            }else
            {
                // Unexcpected exceptions was thrown
                return ReturnValue.ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    public static ReturnValue deleteMovie(Movie movie)
    {
        //Open connection
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        Integer res = 0;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM movie " +
                            "where movie_id = ?");
            pstmt.setInt(1,movie.getId());
            res = pstmt.executeUpdate();
        } catch (SQLException e) {
            // Unexcpected exceptions was thrown
            return ReturnValue.ERROR;
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }


        if( res.equals(0))
        {
            return  ReturnValue.NOT_EXISTS;
        }
        return ReturnValue.OK;
    }

    public static ReturnValue updateMovie(Movie movie)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        int res = 0;
        try {
            pstmt = connection.prepareStatement(
                    "UPDATE movie " +
                            "SET description = ? " +
                            "where movie_id = ?");
            pstmt.setInt(2,movie.getId());
            pstmt.setString(1, movie.getDescription());
            res = pstmt.executeUpdate();
        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue() ||
                    Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                return ReturnValue.BAD_PARAMS;
            }else
            {
                // Unexcpected exceptions was thrown
                return ReturnValue.ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        if( res == 0)
            return ReturnValue.NOT_EXISTS;
        return ReturnValue.OK;
    }

    public static Movie getMovie(Integer movieId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        List<Movie> movies = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement("SELECT * FROM movie " +
                    "WHERE movie_id = ? ");
            pstmt.setInt(1,movieId);
            ResultSet results = pstmt.executeQuery();
            movies = parse_movies(results);
            results.close();

        } catch (SQLException e) {
            return Movie.badMovie();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        if(movies.isEmpty())
        {
            return Movie.badMovie();
        }

        return movies.get(0);
    }



    public static ReturnValue addView(Integer viewerId, Integer movieId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO public.viewed_liked" +
                    " VALUES (?, ?, ?)");
            pstmt.setInt(1, viewerId);
            pstmt.setInt(2, movieId);
            pstmt.setNull(3, Types.OTHER);

            pstmt.execute();

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                return ReturnValue.NOT_EXISTS;
            }else if (Integer.valueOf(e.getSQLState()) == PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                return ReturnValue.ALREADY_EXISTS;
            }else
            {
                // Unexcpected exceptions was thrown
                return ReturnValue.ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    public static ReturnValue removeView(Integer viewerId, Integer movieId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        int res = 0;
        try {
            pstmt = connection.prepareStatement("Delete FROM public.viewed_liked" +
                    " WHERE viewer_id = ? AND movie_id = ?");
            pstmt.setInt(1, viewerId);
            pstmt.setInt(2, movieId);

            res = pstmt.executeUpdate();

        } catch (SQLException e) {
            // Unexcpected exceptions was thrown
            return ReturnValue.ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        if(res == 0 )
        {
            return ReturnValue.NOT_EXISTS;
        }
        return ReturnValue.OK;
    }

    public static Integer getMovieViewCount(Integer movieId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        Integer count = new Integer(0);
        try {
            pstmt = connection.prepareStatement("SELECT COUNT(viewer_id) FROM viewed_liked \n" +
                    "where movie_id = ? ");
            pstmt.setInt(1,movieId);
            ResultSet results = pstmt.executeQuery();
            results.next();
            count = results.getInt(1);
            results.close();

        } catch (SQLException e) {
            return new Integer(0);
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return count;
    }


    public static ReturnValue addMovieRating(Integer viewerId, Integer movieId, MovieRating rating)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        int res = 0;
        try {
            pstmt = connection.prepareStatement("UPDATE public.viewed_liked\n" +
                    " SET liked=?::\"MovieRating\"\n" +
                    " WHERE viewer_id=? AND movie_id=?");
            pstmt.setInt(2, viewerId);
            pstmt.setInt(3, movieId);
            pstmt.setString(1, rating.toString());

            res = pstmt.executeUpdate();

        } catch (SQLException e) {
                // Unexcpected exceptions was thrown
                return ReturnValue.ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        if(res == 0)
            return ReturnValue.NOT_EXISTS;

        return ReturnValue.OK;
    }

    public static ReturnValue removeMovieRating(Integer viewerId, Integer movieId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        int res = 0;
        try {
            pstmt = connection.prepareStatement("UPDATE public.viewed_liked\n" +
                    " SET liked=?\n" +
                    " WHERE viewer_id=? AND movie_id=? AND liked IS NOT NULL");
            pstmt.setInt(2, viewerId);
            pstmt.setInt(3, movieId);
            pstmt.setNull(1, Types.OTHER);

            res = pstmt.executeUpdate();

        } catch (SQLException e) {
            // Unexcpected exceptions was thrown
            return ReturnValue.ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        if(res == 0)
            return ReturnValue.NOT_EXISTS;

        return ReturnValue.OK;
    }

    public static int getMovieLikesCount(int movieId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        Integer count = new Integer(0);
        try {
            pstmt = connection.prepareStatement("SELECT COUNT(viewer_id) FROM viewed_liked \n" +
                    "where movie_id = ? AND liked = 'LIKE' ");
            pstmt.setInt(1,movieId);
            ResultSet results = pstmt.executeQuery();
            results.next();
            count = results.getInt(1);
            results.close();

        } catch (SQLException e) {
            return new Integer(0);
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return count;
    }

    public static int getMovieDislikesCount(int movieId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        Integer count = new Integer(0);
        try {
            pstmt = connection.prepareStatement("SELECT COUNT(viewer_id) FROM viewed_liked \n" +
                    "where movie_id = ? AND liked = 'DISLIKE' ");
            pstmt.setInt(1,movieId);
            ResultSet results = pstmt.executeQuery();
            results.next();
            count = results.getInt(1);
            results.close();

        } catch (SQLException e) {
            return new Integer(0);
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return count;
    }

    public static ArrayList<Integer> getSimilarViewers(Integer viewerId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> res = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement(
                    "SELECT viewer_id FROM\n" +
                            "(SELECT viewer_id, COUNT(movie_id) FROM\n" +
                            "(SELECT * FROM viewed_liked WHERE movie_id IN\n" +
                            "(SELECT movie_id FROM viewed_liked WHERE viewer_id = ?)  AND viewer_id <> 1) AS \n"+
                            " similar_viewers_movies GROUP BY viewer_id) AS similar_counts\n" +
                            "WHERE similar_counts.count > (((SELECT COUNT(movie_id) FROM \n"+
                            "(SELECT movie_id FROM viewed_liked WHERE viewer_id = ?) AS viewer_movies ) *3)/4)");
            pstmt.setInt(1,viewerId);
            pstmt.setInt(2,viewerId);
            ResultSet results = pstmt.executeQuery();
            while (results.next())
            {
                res.add(results.getInt(1));
            }

            results.close();

        } catch (SQLException e) {
            return new ArrayList<Integer>();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return res;
    }


    public static ArrayList<Integer> mostInfluencingViewers()
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> res = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement(
                    " SELECT viewer_id FROM (SELECT viewer_id, COUNT(movie_id) AS count_movie,\n"+
                            " COUNT(liked) AS count_like\n" +
                            " FROM viewed_liked GROUP BY viewer_id\n" +
                            " ORDER BY count_movie DESC, count_like DESC, viewer_id ASC LIMIT 10) AS most_influ");
            ResultSet results = pstmt.executeQuery();
            while (results.next())
            {
                res.add(results.getInt(1));
            }

            results.close();

        } catch (SQLException e) {
            return new ArrayList<Integer>();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return res;
    }


    public static ArrayList<Integer> getMoviesRecommendations(Integer viewerId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> res = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement(
                    "SELECT movie_id FROM (\n" +
                            "SELECT movie_id, COUNT(liked) AS count_likes FROM viewed_liked WHERE liked='LIKE' \n" +
                            " AND movie_id NOT IN (SELECT movie_id FROM viewed_liked WHERE viewer_id = ?)\n" +
                            " AND viewer_id IN (SELECT viewer_id FROM\n" +
                            " (SELECT viewer_id, COUNT(movie_id) FROM\n" +
                            " (SELECT * FROM viewed_liked WHERE movie_id IN\n" +
                            " (SELECT movie_id FROM viewed_liked WHERE viewer_id = ?)  AND viewer_id <> ?) AS\n" +
                            " similar_viewers_movies GROUP BY viewer_id) AS similar_counts\n" +
                            " WHERE similar_counts.count > (((SELECT COUNT(movie_id) FROM \n" +
                            " (SELECT movie_id FROM viewed_liked WHERE viewer_id = ?) AS viewer_movies ) *3)/4))"+
                            " GROUP BY movie_id \n" +
                            " ORDER BY count_likes DESC, movie_id ASC LIMIT 10) AS recomend_with_count\n");
            pstmt.setInt(1,viewerId);
            pstmt.setInt(2,viewerId);
            pstmt.setInt(3,viewerId);
            pstmt.setInt(4,viewerId);
            ResultSet results = pstmt.executeQuery();
            while (results.next())
            {
                res.add(results.getInt(1));
            }

            results.close();

        } catch (SQLException e) {
            return new ArrayList<Integer>();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return res;
    }


    public static ArrayList<Integer> getConditionalRecommendations(Integer viewerId, int movieId)
    {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> res = new ArrayList<>();
        try {
            pstmt = connection.prepareStatement(
                    "SELECT movie_id FROM (\n"+
                            "SELECT movie_id, COUNT(liked) AS count_likes FROM viewed_liked WHERE liked='LIKE' \n" +
                            "AND movie_id NOT IN (SELECT movie_id FROM viewed_liked WHERE viewer_id = ?)\n" +
                            "AND viewer_id IN (SELECT viewer_id FROM\n" +
                            "(SELECT viewer_id, COUNT(movie_id) FROM\n" +
                            "(SELECT * FROM viewed_liked WHERE movie_id IN\n" +
                            "(SELECT movie_id FROM viewed_liked WHERE viewer_id = ?)  AND viewer_id <> ? AND liked = \n" +
                            "(SELECT liked FROM viewed_liked WHERE viewer_id=? AND movie_id=?)) AS\n" +
                            "similar_viewers_movies GROUP BY viewer_id) AS similar_counts\n" +
                            "WHERE similar_counts.count > (((SELECT COUNT(movie_id) FROM \n" +
                            "(SELECT movie_id FROM viewed_liked WHERE viewer_id = ?) AS viewer_movies ) *3)/4)) GROUP BY movie_id \n" +
                            "ORDER BY count_likes DESC, movie_id ASC LIMIT 10) AS similar_ranked_with_count");
            pstmt.setInt(1,viewerId);
            pstmt.setInt(2,viewerId);
            pstmt.setInt(3,viewerId);
            pstmt.setInt(4,viewerId);
            pstmt.setInt(5,movieId);
            pstmt.setInt(6,viewerId);
            ResultSet results = pstmt.executeQuery();
            while (results.next())
            {
                res.add(results.getInt(1));
            }

            results.close();

        } catch (SQLException e) {
            return new ArrayList<Integer>();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return res;
    }

}


